import os
import urlparse
from tempfile import NamedTemporaryFile
import thread
from random import Random
import errno

from xframes.environment import Environment


class Singleton(object):
    """
    A non-thread-safe helper class to ease implementing singletons.
    This should be used as a decorator -- not a metaclass -- to the
    class that should be a singleton.

    The decorated class can define one `__init__` function that
    takes only the `self` argument. Other than that, there are
    no restrictions that apply to the decorated class.

    To get the singleton instance, use the `Instance` method. Trying
    to use `__call__` will result in a `TypeError` being raised.

    Limitations: The decorated class cannot be inherited from.

    """

    def __init__(self, decorated):
        self._decorated = decorated

    # noinspection PyAttributeOutsideInit,PyPep8Naming
    def Instance(self):
        """
        Returns the singleton instance. Upon its first call, it creates a
        new instance of the decorated class and calls its `__init__` method.
        On all subsequent calls, the already created instance is returned.

        """
        try:
            return self._instance
        except AttributeError:
            self._instance = self._decorated()
            return self._instance

    def __call__(self):
        raise TypeError('Singletons must be accessed through `Instance()`.')

    def __instancecheck__(self, inst):
        return isinstance(inst, self._decorated)


class UriError(Exception):
    pass


# noinspection PyArgumentList
def _parse_uri(uri):
    parsed = urlparse.urlparse(uri)
    if parsed.scheme == 'hdfs':
        if parsed.hostname == '' or parsed.port == '':
            raise UriError('HDFS URI must have hostname and port: ' + uri)
        return parsed
    if parsed.scheme == '':
        return urlparse.ParseResult('file', parsed.netloc, parsed.path, '', '', '')
    return parsed


@Singleton
class _HdfsConnection(object):
    def __init__(self):
        env = Environment.create()
        config_context = env.get_config_items('webhdfs')
        if config_context is not None and 'port' in config_context:
            self.port = config_context['port']
            self.user = config_context['user'] if 'user' in config_context else 'root'
            self.use_kerberos = config_context['kerberos']
        else:
            self.port = None
            self.user = None
            self.kerberos_flag = None

    def hdfs_connection(self, parsed_uri):
        # uses the hostname in the uri, replaces port by configured port
        client_uri = 'http://{}:{}'.format(parsed_uri.hostname, self.port)
        if self.use_kerberos:
            from hdfs.ext import kerberos as hdfs_client
            return hdfs_client.KerberosClient(client_uri, user=self.user)
        else:
            from hdfs import client as hdfs_client
            return hdfs_client.InsecureClient(client_uri)

    def has_hdfs(self):
        return self.port is not None


def _make_hdfs_connection(parsed_uri):
    return _HdfsConnection.Instance().hdfs_connection(parsed_uri)


# This code was adapted from the NamedTemporaryFile code in package tempfile
_allocate_lock = thread.allocate_lock
_once_lock = _allocate_lock()


class _RandomNameSequence:
    """An instance of _RandomNameSequence generates an endless
    sequence of unpredictable strings which can safely be incorporated
    into file names.  Each string is six characters long.  Multiple
    threads can safely use the same instance at the same time.

    _RandomNameSequence is an iterator."""

    characters = ("abcdefghijklmnopqrstuvwxyz" +
                  "ABCDEFGHIJKLMNOPQRSTUVWXYZ" +
                  "0123456789_")

    def __init__(self):
        self.mutex = _allocate_lock()
        self.normcase = os.path.normcase
        self._rng = None
        self._rng_pid = None

    @property
    def rng(self):
        cur_pid = os.getpid()
        if cur_pid != getattr(self, '_rng_pid', None):
            self._rng = Random()
            self._rng_pid = cur_pid
        return self._rng

    def __iter__(self):
        return self

    def next(self):
        m = self.mutex
        c = self.characters
        choose = self.rng.choice

        m.acquire()
        try:
            letters = [choose(c) for _ in "123456"]
        finally:
            m.release()

        return self.normcase(''.join(letters))


_name_sequence = None


def _get_candidate_names():
    """Common setup sequence for all user-callable interfaces."""
    global _name_sequence
    if _name_sequence is None:
        _once_lock.acquire()
        try:
            if _name_sequence is None:
                _name_sequence = _RandomNameSequence()
        finally:
            _once_lock.release()
    return _name_sequence


def _named_temp_file(parsed_uri, directory=None, prefix=None, suffix=None):
    """ Return a temp filename in hdfs.  """
    directory = directory or 'tmp'
    prefix = prefix or 'tmp'
    suffix = suffix or ''
    hdfs_prefix = '{}://{}:{}'.format(parsed_uri.scheme, parsed_uri.hostname, parsed_uri.port)

    tmp_max = 10000
    names = _get_candidate_names()

    for seq in xrange(tmp_max):
        name = names.next()
        hdfs_path = '/'.join([hdfs_prefix, directory, prefix + name + suffix])
        try:
            if not is_file(hdfs_path):
                return hdfs_path
        except OSError, e:
            if e.errno == errno.EEXIST:
                continue  # try again
            raise

    raise IOError(errno.EEXIST, "No usable hdfs temporary file name found.")


# External Interface


def has_hdfs():
    return _HdfsConnection.Instance().has_hdfs()


def open_file(uri, mode='r'):
    # Opens a file for read or write
    # The caller should wrapped this in with statement to make sure it is closed after use

    parsed_uri = _parse_uri(uri)
    if parsed_uri.scheme == 'file':
        return open(parsed_uri.path, mode)
    elif parsed_uri.scheme == 'hdfs':
        hdfs_connection = _make_hdfs_connection(parsed_uri)
        if mode.startswith('r'):
            return hdfs_connection.read(parsed_uri.path)
        elif mode.startswith('w'):
            hdfs_connection.makedirs(os.path.dirname(parsed_uri.path))
            hdfs_connection.delete(parsed_uri.path, recursive=True)
            return hdfs_connection.write(parsed_uri.path)
        else:
            raise IOError('Invalid open mode for HDFS: '.format(mode))
    elif parsed_uri.scheme == 's3':
        raise UriError('S3 not supported')
    else:
        raise UriError('Unknown URI scheme: {}'.format(parsed_uri.scheme))


def delete(uri):
    parsed_uri = _parse_uri(uri)
    if parsed_uri.scheme == 'file':
        from xframes.util import delete_file_or_dir
        delete_file_or_dir(parsed_uri.path)
    elif parsed_uri.scheme == 'hdfs':
        hdfs_connection = _make_hdfs_connection(parsed_uri)
        hdfs_connection.delete(parsed_uri.path, recursive=True)
    elif parsed_uri.scheme == 'hive':
        pass
    else:
        raise UriError('Unknown URI scheme: {}'.format(parsed_uri.scheme))


def temp_file_name(uri):
    # make it on the same filesystem as path
    parsed_uri = _parse_uri(uri)
    if parsed_uri.scheme == 'file':
        temp_file = NamedTemporaryFile(delete=True)
        temp_file.close()
        return temp_file.name
    elif parsed_uri.scheme == 'hdfs':
        return _named_temp_file(parsed_uri)
    else:
        raise UriError('Unknown URI scheme: {}'.format(parsed_uri.scheme))


def is_file(uri):
    parsed_uri = _parse_uri(uri)
    if parsed_uri.scheme == 'file':
        return os.path.isfile(parsed_uri.path)
    elif parsed_uri.scheme == 'hdfs':
        hdfs_connection = _make_hdfs_connection(parsed_uri)
        status = hdfs_connection.status(parsed_uri.path, strict=False)
        return status is not None and status['type'] == 'FILE'
    else:
        raise UriError('Unknown URI scheme: {}'.format(parsed_uri.scheme))


def is_dir(uri):
    parsed_uri = _parse_uri(uri)
    if parsed_uri.scheme == 'file':
        return os.path.isdir(parsed_uri.path)
    elif parsed_uri.scheme == 'hdfs':
        hdfs_connection = _make_hdfs_connection(parsed_uri)
        status = hdfs_connection.status(parsed_uri.path, strict=False)
        return status is not None and status['type'] == 'DIRECTORY'
    else:
        raise UriError('Unknown URI scheme: {}'.format(parsed_uri.scheme))


def exists(uri):
    parsed_uri = _parse_uri(uri)
    if parsed_uri.scheme == 'file':
        return os.path.exists(parsed_uri.path)
    elif parsed_uri.scheme == 'hdfs':
        hdfs_connection = _make_hdfs_connection(parsed_uri)
        status = hdfs_connection.status(parsed_uri.path, strict=False)
        return status is not None
    else:
        raise UriError('Unknown scheme: {}'.format(parsed_uri.scheme))


def make_dir(uri):
    parsed_uri = _parse_uri(uri)
    if parsed_uri.scheme == 'file':
        try:
            os.makedirs(parsed_uri.path)
        except OSError:
            raise UriError('Cannot make directory: {}'.format(uri))
    elif parsed_uri.scheme == 'hdfs':
        hdfs_connection = _make_hdfs_connection(parsed_uri)
        status = hdfs_connection.makedirs(parsed_uri.path)
        return status is not None
    else:
        raise UriError('Unknown scheme: {}'.format(parsed_uri.scheme))


def list_dir(uri, status=False):
    parsed_uri = _parse_uri(uri)
    if parsed_uri.scheme == 'file':
        if status:
            files = os.listdir(parsed_uri.path)
            return [(f, os.stat(f)) for f in files]
        else:
            return os.listdir(parsed_uri.path)
    elif parsed_uri.scheme == 'hdfs':
        hdfs_connection = _make_hdfs_connection(parsed_uri)
        files = hdfs_connection.list(parsed_uri.path, status=status)
        return files
    else:
        raise UriError('Unknown scheme: {}'.format(parsed_uri.scheme))


def m_time(uri):
    parsed_uri = _parse_uri(uri)
    if parsed_uri.scheme == 'file':
        return os.stat(parsed_uri.path).st_mtime
    elif parsed_uri.scheme == 'hdfs':
        hdfs_connection = _make_hdfs_connection(parsed_uri)
        file_time = hdfs_connection.status(parsed_uri.path)['modificationTime'] / 1000.0
        return file_time
    else:
        raise UriError('Unknown scheme: {}'.format(parsed_uri.scheme))


def length(uri):
    parsed_uri = _parse_uri(uri)
    if parsed_uri.scheme == 'file':
        if os.path.isdir(parsed_uri.path):
            # a dir
            files = os.listdir(parsed_uri.path)

            def file_len(filename):
                return os.stat(os.path.join(parsed_uri.path, filename)).st_size
            lengths = [file_len(f) for f in files]
            return sum(lengths)
        else:
            # a file
            return os.stat(parsed_uri.path).st_size
    elif parsed_uri.scheme == 'hdfs':
        hdfs_connection = _make_hdfs_connection(parsed_uri)
        status = hdfs_connection.status(parsed_uri.path, strict=False)
        if status is None:
            return 0
        if status['type'] == 'DIRECTORY':
            # a dir
            files = hdfs_connection.list(parsed_uri.path, status=True)
            lengths = [f[1]['length'] for f in files]
            return sum(lengths)
        else:
            # a file
            return hdfs_connection.status(parsed_uri.path)['length']
    else:
        raise UriError('Unknown scheme: {}'.format(parsed_uri.scheme))

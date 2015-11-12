import os
import re
from tempfile import NamedTemporaryFile
import thread
from random import Random
import errno

from xframes.environment import Environment
from xframes.singleton import Singleton


class HdfsError(Exception):
    pass


@Singleton
class _HdfsConnection(object):
    def __init__(self):
        env = Environment.create()
        config_context = env.get_config_items('hdfs')
        if config_context is not None and 'port' in config_context:
            self.port = config_context['port']
            self.user = config_context['user'] if 'user' in config_context else 'root'
        else:
            self.port = None
            self.user = None

    def hdfs_connection(self, path):
        host, port = _get_hdfs_host_port(path)
        # uses the host in the path, replaces port by configured port
        client_path = 'http://{}:{}'.format(host, self.port)
        from hdfs import client as hdfs_client
        return hdfs_client.InsecureClient(client_path, user=self.user)

    def has_hdfs(self):
        return self.port is not None


def _is_hdfs_uri(path):
    return path.startswith('hdfs://')


def _make_hdfs_connection(path):
    return _HdfsConnection.Instance().hdfs_connection(path)

_hdfs_re = 'hdfs://([A-Za-z0-9_.-]+):([0-9]+)(.+)'


def _get_hdfs_path(path):
    match = re.match(_hdfs_re, path)
    if match is None:
        raise HdfsError('Invalid hdfs path: {}'.format(path))
    if not has_hdfs():
        raise HdfsError('HDFS is not configured.  See xxx.')
    return match.group(3)


def _get_hdfs_host_port(path):
    match = re.match(_hdfs_re, path)
    if match is None:
        raise HdfsError('Invalid hdfs path: {}'.format(path))
    if not has_hdfs():
        raise HdfsError('HDFS is not configured.  See xxx.')
    return match.group(1, 2)


def get_hdfs_prefix(path):
    match = re.match('(hdfs://[A-Za-z0-9_-]+:[0-9]+)', path)
    if match is None:
        raise HdfsError('Invalid hdfs path: {}'.format(path))
    if not has_hdfs():
        raise HdfsError('HDFS is not configured.  See xxx.')
    return match.group(1)

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


def _named_temp_file(path, directory=None, prefix=None, suffix=None):
    """ Return a temp filename in hdfs.  """
    directory = directory or 'tmp'
    prefix = prefix or 'tmp'
    suffix = suffix or ''
    hdfs_prefix = get_hdfs_prefix(path)

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


def open_file(path, mode='r'):
    # Opens a file for read
    # This should be wrapped in with statement to make sure it is closed after use
    if not _is_hdfs_uri(path):
        return open(path, mode)
    else:
        hdfs_connection = _make_hdfs_connection(path)
        hdfs_path = _get_hdfs_path(path)
        if mode.startswith('r'):
            return hdfs_connection.read(hdfs_path)
        elif mode.startswith('w'):
            delete(path)
            return hdfs_connection.write(hdfs_path)
        else:
            raise IOError('Invalid open mode for HDFS.')


def delete(path):
    if not _is_hdfs_uri(path):
        from xframes.util import delete_file_or_dir
        delete_file_or_dir(path)
    else:
        hdfs_connection = _make_hdfs_connection(path)
        hdfs_path = _get_hdfs_path(path)
        hdfs_connection.delete(hdfs_path, recursive=True)


def temp_file_name(path):
    # make it on the same filesystem as path
    if not _is_hdfs_uri(path):
        temp_file = NamedTemporaryFile(delete=True)
        temp_file.close()
        return temp_file.name
    else:
        return _named_temp_file(path)


def is_file(path):
    if not _is_hdfs_uri(path):
        return os.path.isfile(path)
    else:
        hdfs_connection = _make_hdfs_connection(path)
        hdfs_path = _get_hdfs_path(path)
        status = hdfs_connection.status(hdfs_path, strict=False)
        return status is not None and status['type'] == 'FILE'


def is_dir(path):
    if not _is_hdfs_uri(path):
        return os.path.isdir(path)
    else:
        hdfs_connection = _make_hdfs_connection(path)
        hdfs_path = _get_hdfs_path(path)
        status = hdfs_connection.status(hdfs_path, strict=False)
        return status is not None and status['type'] == 'DIRECTORY'

def exists(path):
    if not _is_hdfs_uri(path):
        return os.path.exists(path)
    else:
        hdfs_connection = _make_hdfs_connection(path)
        hdfs_path = _get_hdfs_path(path)
        status = hdfs_connection.status(hdfs_path, strict=False)
        return status is not None

def list_dir(path):
    if not _is_hdfs_uri(path):
        return os.listdir(path)
    else:
        hdfs_connection = _make_hdfs_connection(path)
        hdfs_path = _get_hdfs_path(path)
        files = hdfs_connection.list(hdfs_path)
        return files

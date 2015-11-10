import os
import pickle
import shutil
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
class HdfsConnection(object):
    def __init__(self):
        env = Environment.create()
        config_context = env.get_config_items('hdfs')
        if config_context is not None and 'host' in config_context and 'port' in config_context:
            # do not import unless needed
            from hdfs import client as hdfs_client
            host = config_context['host']
            port = config_context['port']
            client_path = 'http://{}:{}'.format(host, port)
            self.connection = hdfs_client.InsecureClient(client_path, user='root')
        else:
            self.connection = None

    def hdfs_connection(self):
        return self.connection


_allocate_lock = thread.allocate_lock
_once_lock = _allocate_lock()


class RandomNameSequence:
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
            letters = [choose(c) for dummy in "123456"]
        finally:
            m.release()

        return self.normcase(''.join(letters))


_name_sequence = None


def get_candidate_names():
    """Common setup sequence for all user-callable interfaces."""

    global _name_sequence
    if _name_sequence is None:
        _once_lock.acquire()
        try:
            if _name_sequence is None:
                _name_sequence = RandomNameSequence()
        finally:
            _once_lock.release()
    return _name_sequence


def named_temp_file(hdfs_connection, path, suffix=None, prefix=None, dir=None):
    dir = dir or 'tmp'
    prefix = prefix or 'tmp'
    suffix = suffix or ''
    host, port = get_hdfs_host_port(path)
    hdfs_prefix = 'hdfs://{}:{}'.format(host, port)

    TMP_MAX = 10000
    names = get_candidate_names()

    for seq in xrange(TMP_MAX):
        name = names.next()
        #hdfs_path = os.path.join(hdfs_prefix, dir, prefix + name + suffix)
        hdfs_path = '/'.join([hdfs_prefix, dir, prefix + name + suffix])
        try:
            status = hdfs_connection.status(hdfs_path, strict=False)
            if status is None:
                return hdfs_path
        except OSError, e:
            if e.errno == errno.EEXIST:
                continue # try again
            raise

    raise IOError, (errno.EEXIST, "No usable hdfs temporary file name found")


def has_hdfs():
    return HdfsConnection.Instance().hdfs_connection() is not None


def is_hdfs_uri(path):
    return path.startswith('hdfs://')


def make_hdfs_connection():
    return HdfsConnection.Instance().hdfs_connection()

hdfs_re = 'hdfs://([A-Za-z0-9_-]+):([0-9]+)(.+)'


def get_hdfs_path(path):
    match = re.match(hdfs_re, path)
    if match is None:
        raise HdfsError('Invalid hdfs path: {}'.format(path))
    if not has_hdfs():
        raise HdfsError('HDFS is not configured.  See xxx.')
    return match.group(3)


def get_hdfs_host_port(path):
    match = re.match(hdfs_re, path)
    if match is None:
        raise HdfsError('Invalid hdfs path: {}'.format(path))
    if not has_hdfs():
        raise HdfsError('HDFS is not configured.  See xxx.')
    return match.group(1, 2)


def save_csv_data(csv_data):
    # Make a CSV file from an RDD

    # this will ensure that we get everything in one fie
    data = csv_data.repartition(1)

    # save the data in a part file
    temp_file = NamedTemporaryFile(delete=True)
    temp_file.close()
    data.saveAsTextFile(temp_file.name)
    data_path = os.path.join(temp_file.name, 'part-00000')
    return data_path, temp_file.name


def write_file_local(in_path, out_path, heading):
    # copy the part file to the output file
    delete_path(out_path)
    with open(out_path, 'w') as f:
        f.write(heading)
        with open(in_path, 'r') as rd:
            shutil.copyfileobj(rd, f)


def write_file_hdfs(hdfs_connection, in_path, out_path, heading):
    hdfs_connection.delete(out_path, recursive=True)
    hdfs_in_path = get_hdfs_path(in_path)
    hdfs_out_path = get_hdfs_path(out_path)
    with hdfs_connection.write(hdfs_out_path) as f:
        f.write(heading)
        with hdfs_connection.read(hdfs_in_path) as rd:
            shutil.copyfileobj(rd, f)


# External Interface
def load_pickle_file(path):
    if not is_hdfs_uri(path):
        with open(path) as f:
            return pickle.load(f)
    else:
        hdfs_connection = make_hdfs_connection()
        hdfs_path = get_hdfs_path(path)
        with hdfs_connection.read(hdfs_path) as f:
            return pickle.load(f)


def dump_pickle_file(path, data):
    if not is_hdfs_uri(path):
        with open(path, 'w') as f:
            # TODO detect filesystem errors
            pickle.dump(data, f)
    else:
        hdfs_connection = make_hdfs_connection()
        hdfs_path = get_hdfs_path(path)
        with hdfs_connection.write(hdfs_path) as f:
            pickle.dump(data, f)


def write_file(in_path, out_path, heading):
    if not is_hdfs_uri(out_path):
        write_file_local(in_path, out_path, heading)
    else:
        hdfs_connection = make_hdfs_connection()
#        hdfs_in_path = get_hdfs_path(in_path)
#        hdfs_out_path = get_hdfs_path(out_path)
#        write_file_hdfs(hdfs_connection, hdfs_in_path, hdfs_out_path, heading)
        write_file_hdfs(hdfs_connection, in_path, out_path, heading)


def open_file(path, mode='r'):
    # Opens a file for read
    # This should be wrapped in with statement to make sure it is closed after use
    if not is_hdfs_uri(path):
        return open(path, mode)
    else:
        hdfs_connection = make_hdfs_connection()
        hdfs_path = get_hdfs_path(path)
        if mode == 'r':
            return hdfs_connection.read(hdfs_path)
        elif mode == 'w':
            return hdfs_connection.write(hdfs_path)
        else:
            raise IOError('Invalid open mode for HDFS.')


def delete_path(path):
    if not is_hdfs_uri(path):
        from xframes.util import delete_file_or_dir
        delete_file_or_dir(path)
    else:
        hdfs_connection = make_hdfs_connection()
        hdfs_path = get_hdfs_path(path)
        hdfs_connection.delete(hdfs_path, recursive=True)


def temp_file_name(path):
    # make it on the same filesystem as path
    if not is_hdfs_uri(path):
        temp_file = NamedTemporaryFile(delete=True)
        temp_file.close()
        return temp_file.name
    else:
        hdfs_connection = make_hdfs_connection()
        return named_temp_file(hdfs_connection, path)


def is_file(path):
    if not is_hdfs_uri(path):
        return os.path.isfile(path)
    else:
        hdfs_connection = make_hdfs_connection()
        hdfs_path = get_hdfs_path(path)
        status = hdfs_connection.status(hdfs_path, strict=False)
        # returns True even if the path is a directory
        return status is not None

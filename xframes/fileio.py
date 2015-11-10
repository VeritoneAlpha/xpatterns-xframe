import os
import pickle
import shutil
import re
from tempfile import NamedTemporaryFile

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


def has_hdfs():
    return HdfsConnection.Instance().hdfs_connection() is not None


def is_hdfs_uri(path):
    return path.startswith('hdfs://')


def make_hdfs_connection(path):
    return HdfsConnection.Instance().hdfs_connection()


def make_hdfs_path(path):
    match = re.match('hdfs://([A-Za-z0-9_-]+):([0-9]+)(.+)', path)
    if match is None:
        raise HdfsError('Invalid hdfs path: {}'.format(path))
    if not has_hdfs():
        raise HdfsError('HDFS is not configured.  See xxx.')
    return match.group(3)


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


def write_file_local(in_path, out_path, temp_file_name, heading):
    # copy the part file to the output file
    delete_path(out_path)
    with open(out_path, 'w') as f:
        f.write(heading)
        with open(in_path, 'r') as rd:
            shutil.copyfileobj(rd, f)

    # clean up part file
    delete_path(temp_file_name)


def write_file_hdfs(hdfs_connection, in_path, out_path, temp_file_name, heading):
    hdfs_connection.delete(out_path, recursive=True)
    with hdfs_connection.write(out_path) as f:
        f.write(heading)
        with hdfs_connection.read(in_path) as rd:
            shutil.copyfileobj(rd, f)
    hdfs_connection.delete(temp_file_name)


# External Interface
def load_pickle_file(path):
    if not is_hdfs_uri(path):
        with open(path) as f:
            return pickle.load(f)
    else:
        hdfs_connection = make_hdfs_connection(path)
        hdfs_path = make_hdfs_path(path)
        with hdfs_connection.read(hdfs_path) as f:
            return pickle.load(f)


def dump_pickle_file(path, data):
    if not is_hdfs_uri(path):
        with open(path, 'w') as f:
            # TODO detect filesystem errors
            pickle.dump(data, f)
    else:
        hdfs_connection = make_hdfs_connection(path)
        hdfs_path = make_hdfs_path(path)
        with hdfs_connection.write(hdfs_path) as f:
            pickle.dump(data, f)


def write_file(in_path, out_path, temp_file_name, heading):
    if not is_hdfs_uri(out_path):
        write_file_local(in_path, out_path, temp_file_name, heading)
    else:
        hdfs_connection = make_hdfs_connection(in_path)
        hdfs_in_path = make_hdfs_path(in_path)
        hdfs_out_path = make_hdfs_path(in_path)
        write_file_hdfs(hdfs_connection, hdfs_in_path, hdfs_out_path, temp_file_name, heading)


def open_file(path, mode='r'):
    # Opens a file for read
    # This should be wrapped in with statement to make sure it is closed after use
    if not is_hdfs_uri(path):
        return open(path, mode)
    else:
        hdfs_connection = make_hdfs_connection(path)
        hdfs_path = make_hdfs_path(path)
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
        hdfs_connection = make_hdfs_connection(path)
        hdfs_path = make_hdfs_path(path)
        hdfs_connection.delete(hdfs_path, recursive=True)

def is_file(path):
    if not is_hdfs_uri(path):
        return os.path.isfile(path)
    else:
        hdfs_connection = make_hdfs_connection(path)
        hdfs_path = make_hdfs_path(path)
        status = hdfs_connection.status(hdfs_path, strict=False)
        # returns True even if the path is a directory
        return status is not None

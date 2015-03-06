"""
This module defines top level utility functions for GraphLab.
"""
import urllib as _urllib
import urllib2 as _urllib2
import sys as _sys
import os as _os
import re as _re
from zipfile import ZipFile as _ZipFile
import bz2 as _bz2
import tarfile as _tarfile
import ConfigParser as _ConfigParser
import itertools as _itertools

#from graphlab.connect.aws._ec2 import get_credentials as _get_aws_credentials
#import graphlab.connect as _mt
#import graphlab.connect.main as _glconnect
#import graphlab.connect.server as _server
#import graphlab.version_info
#from pkg_resources import parse_version

import logging as _logging

__LOGGER__ = _logging.getLogger(__name__)

def get_credentials():
    """
    Returns the values stored in the AWS credential environment variables.
    Returns the value stored in the AWS_ACCESS_KEY_ID environment variable and
    the value stored in the AWS_SECRET_ACCESS_KEY environment variable.

    Returns
    -------
    out : tuple [string]
        The first string of the tuple is the value of the AWS_ACCESS_KEY_ID
        environment variable. The second string of the tuple is the value of the
        AWS_SECRET_ACCESS_KEY environment variable.


    Examples
    --------
    >>> xpatterns.util.get_credentials()
    ('RBZH792CTQPP7T435BGQ', '7x2hMqplWsLpU/qQCN6xAPKcmWo46TlPJXYTvKcv')
    """

    if (not 'AWS_ACCESS_KEY_ID' in _os.environ):
        raise KeyError('No access key found. Please set the environment variable AWS_ACCESS_KEY_ID.')
    if (not 'AWS_SECRET_ACCESS_KEY' in _os.environ):
        raise KeyError('No secret key found. Please set the environment variable AWS_SECRET_ACCESS_KEY.')
    return (_os.environ['AWS_ACCESS_KEY_ID'], _os.environ['AWS_SECRET_ACCESS_KEY'])

def make_internal_url(url):
    """
    Takes a user input url string and translates into url relative to the server process.
    - URL to a local location begins with "local://" or has no "*://" modifier.
      If the server is local, returns the absolute path of the url.
      For example: "local:///tmp/foo" -> "/tmp/foo" and "./foo" -> os.path.abspath("./foo").
      If the server is not local, raise NotImplementedError.
    - URL to a server location begins with "remote://".
      Returns the absolute path after the "remote://" modifier.
      For example: "remote:///tmp/foo" -> "/tmp/foo".
    - URL to a s3 location begins with "s3://":
      Returns the s3 URL with credentials filled in using graphlab.aws.get_aws_credential().
      For example: "s3://mybucket/foo" -> "s3://$AWS_ACCESS_KEY_ID:$AWS_SECRET_ACCESS_KEY:mybucket/foo".
    - URL to other remote locations, e.g. http://, will remain as is.
    - Expands ~ to $HOME

    Parameters
    ----------
    string
        A URL (as described above).

    Raises
    ------
    ValueError
        If a bad url is provided.
    """
    if not url:
        raise ValueError('Invalid url: %s' % url)

    # The final file path on server.
    path_on_server = None

    # Try to split the url into (protocol, path).
    urlsplit = url.split("://")
    if len(urlsplit) == 2:
        protocol, path = urlsplit
        if not path:
            raise ValueError('Invalid url: %s' % url)
        if protocol in ['http', 'https']:
            # protocol is a remote url not on server, just return
            return url
        elif protocol == 'hdfs':
            if isinstance(_glconnect.get_server(), _server.LocalServer) and not _server._get_hadoop_class_path():
                raise ValueError("HDFS URL is not supported because Hadoop not found. Please make hadoop available from PATH or set the environment variable HADOOP_HOME and try again.")
            else:
                return url
        elif protocol == 's3':
            if len(path.split(":")) == 3:
            # s3 url already contains secret key/id pairs, just return
                return url
            else:
            # s3 url does not contain secret key/id pair, query the environment variables
                (k, v) = get_credentials()
#                return 's3n://' + k + ':' + v + '@' + path
                return 's3n://' + path
        elif protocol == 'remote':
        # url for files on the server
            path_on_server = path
        elif protocol == 'local':
        # url for files on local client, check if we are connecting to local server
            if (isinstance(_glconnect.get_server(), _server.LocalServer)):
                path_on_server = path
            else:
                raise ValueError('Cannot use local URL when connecting to a remote server.')
        else:
            raise ValueError('Invalid url protocol %s. Supported url protocols are: remote://, local://, s3://, https:// and hdfs://' % protocol)
    elif len(urlsplit) == 1:
        # expand ~ to $HOME
        url = _os.path.expanduser(url)
        # url for files on local client, check if we are connecting to local server
# TEMP
        if True:
#        if (isinstance(_glconnect.get_server(), _server.LocalServer)):
            path_on_server = url
        else:
            raise ValueError('Cannot use local URL when connecting to a remote server.')
    else:
        raise ValueError('Invalid url: %s' % url)

    if path_on_server:
        return _os.path.abspath(_os.path.expanduser(path_on_server))
    else:
        raise ValueError('Invalid url: %s' % url)


def download_dataset(url_str, extract=True, force=False, output_dir="."):
    """Download a remote dataset and extract the contents.

    Parameters
    ----------

    url_str : string
        The URL to download from

    extract : bool
        If true, tries to extract compressed file (zip/gz/bz2)

    force : bool
        If true, forces to retry the download even if the downloaded file already exists.

    output_dir : string
        The directory to dump the file. Defaults to current directory.
    """
    fname = output_dir + "/" + url_str.split("/")[-1]
    #download the file from the web
    if not _os.path.isfile(fname) or force:
        print "Downloading file from: ", url_str
        _urllib.urlretrieve(url_str, fname)
        if extract and fname[-3:] == "zip":
            print "Decompressing zip archive", fname
            _ZipFile(fname).extractall(output_dir)
        elif extract and fname[-6:] == ".tar.gz":
            print "Decompressing tar.gz archive", fname
            _tarfile.TarFile(fname).extractall(output_dir)
        elif extract and fname[-7:] == ".tar.bz2":
            print "Decompressing tar.bz2 archive", fname
            _tarfile.TarFile(fname).extractall(output_dir)
        elif extract and fname[-3:] == "bz2":
            print "Decompressing bz2 archive: ", fname
            outfile = open(fname.split(".bz2")[0], "w")
            print "Output file: ", outfile
            for line in _bz2.BZ2File(fname, "r"):
                outfile.write(line)
            outfile.close()
    else:
        print "File is already downloaded."


__GLCREATE_CURRENT_VERSION_URL__ = "http://graphlab.com/files/glcreate_current_version"

def get_newest_version(timeout=5, _url=__GLCREATE_CURRENT_VERSION_URL__):
    """
    Returns the version of GraphLab Create currently available from graphlab.com.
    Will raise an exception if we are unable to reach the graphlab.com servers.

    timeout: int
        How many seconds to wait for the remote server to respond

    url: string
        The URL to go to to check the current version.
    """
    request = _urllib2.urlopen(url=_url, timeout=timeout)
    version = request.read()
    __LOGGER__.debug("current_version read %s" % version)
    return version


def perform_version_check(configfile=(_os.path.join(_os.path.expanduser("~"), ".graphlab", "config")),
                          _url=__GLCREATE_CURRENT_VERSION_URL__,
                          _outputstream=_sys.stderr):
    """
    Checks if currently running version of GraphLab is less than the version
    available from graphlab.com. Prints a message if the graphlab.com servers
    are reachable, and the current version is out of date. Does nothing
    otherwise.

    If the configfile contains a key "skip_version_check" in the Product
    section with non-zero value, this function does nothing.

    Also returns True if a message is printed, and returns False otherwise.
    """
    skip_version_check = False
    try:
        if (_os.path.isfile(configfile)):
            config = _ConfigParser.ConfigParser()
            config.read(configfile)
            section = 'Product'
            key = 'skip_version_check'
            skip_version_check = config.getboolean(section, key)
            __LOGGER__.debug("skip_version_check=%s" % str(skip_version_check))
    except:
        # eat all errors
        pass

    # skip version check set. Quit
    if not skip_version_check:
        try:
            latest_version = get_newest_version(timeout=1, _url=_url).strip()
            if parse_version(latest_version) > parse_version(graphlab.version_info.version):
                msg = ("A newer version of GraphLab Create (v%s) is available! "
                       "Your current version is v%s.\n"
                       "You can use pip to upgrade the graphlab-create package. "
                       "For more information see http://graphlab.com/products/create/upgrade.") % (latest_version, graphlab.version_info.version)
                _outputstream.write(msg)
                return True
        except:
            # eat all errors
            pass
    return False


def is_directory_archive(path):
    """
    Utiilty function that returns True if the path provided is a directory that has an SFrame or SGraph in it.

    SFrames are written to disk as a directory archive, this function identifies if a given directory is an archive
    for an SFrame.

    Parameters
    ----------
    path : string
        Directory to evaluate.

    Returns
    -------
    True if path provided is an archive location, False otherwise
    """
    if path is None:
        return False

    if not _os.path.isdir(path):
        return False

    ini_path = _os.path.join(path, 'dir_archive.ini')

    if not _os.path.exists(ini_path):
        return False

    if _os.path.isfile(ini_path):
        return True

    return False


def get_archive_type(path):
    """
    Returns the contents type for the provided archive path.

    Parameters
    ----------
    path : string
        Directory to evaluate.

    Returns
    -------
    Returns a string of: sframe, sgraph, raises TypeError for anything else
    """
    if not is_directory_archive(path):
        raise TypeError('Unable to determine the type of archive at path: %s' % path)

    try:
        ini_path = _os.path.join(path, 'dir_archive.ini')
        parser = _ConfigParser.SafeConfigParser()
        parser.read(ini_path)

        contents = parser.get('metadata', 'contents')
        return contents
    except Exception as e:
        raise TypeError('Unable to determine type of archive for path: %s' % path, e)

def get_environment_config():
    """
    Returns all the GraphLab configuration variables that can only be set
    via environment variables.

    GRAPHLAB_FILEIO_WRITER_BUFFER_SIZE
      The file write buffer size.

    GRAPHLAB_FILEIO_READER_BUFFER_SIZE
      The file read buffer size.

    OMP_NUM_THREADS
      The maximum number of threads to use for parallel processing.

    Parameters
    ----------
    None

    Returns
    -------
    Returns a dictionary of {key:value,..}
    """
    unity = _glconnect.get_unity()
    return unity.list_globals(False)

def get_runtime_config():
    """
    Returns all the GraphLab configuration variables that can be set at runtime.
    See :py:func:`graphlab.set_runtime_config()` to set these values and for
    documentation on the effect of each variable.

    Parameters
    ----------
    None

    Returns
    -------
    Returns a dictionary of {key:value,..}
    """
    unity = _glconnect.get_unity()
    return unity.list_globals(True)

def set_runtime_config(name, value):
    """
    Sets a runtime configuration value. These configuration values are also
    read from environment variables at program startup if available. See
    :py:func:`graphlab.get_runtime_config()` to get the current values for
    each variable.

    The default configuration is conservatively defined for machines with about
    4-8GB of RAM.

    *Basic Configuration Variables*

    GRAPHLAB_CACHE_FILE_LOCATIONS:
      The directory in which intermediate SFrames/SArray are stored.
      For instance "/var/tmp".  Multiple directories can be specified separated
      by a colon (ex: "/var/tmp:/tmp") in which case intermediate SFrames will
      be striped across both directories (useful for specifying multiple disks).
      Defaults to /var/tmp if the directory exists, /tmp otherwise.

    GRAPHLAB_FILEIO_MAXIMUM_CACHE_CAPACITY:
      The maximum amount of memory which will be occupied by *all* intermediate
      SFrames/SArrays. Once this limit is exceeded, SFrames/SArrays will be
      flushed out to temporary storage (as specified by
      GRAPHLAB_CACHE_FILE_LOCATIONS). On large systems increasing this as well
      as GRAPHLAB_FILEIO_MAXIMUM_CACHE_CAPACITY_PER_FILE can improve performance
      significantly. Defaults to 2147483648 bytes (2GB).

    GRAPHLAB_FILEIO_MAXIMUM_CACHE_CAPACITY_PER_FILE:
      The maximum amount of memory which will be occupied by any individual
      intermediate SFrame/SArray. Once this limit is exceeded, the
      SFrame/SArray will be flushed out to temporary storage (as specified by
      GRAPHLAB_CACHE_FILE_LOCATIONS). On large systems, increasing this as well
      as GRAPHLAB_FILEIO_MAXIMUM_CACHE_CAPACITY can improve performance
      significantly for large SFrames. Defaults to 134217728 bytes (128MB).

    *Advanced Configuration Variables*

    GRAPHLAB_SFRAME_FILE_HANDLE_POOL_SIZE:
      The maximum number of file handles to use when reading SFrames/SArrays.
      Once this limit is exceeded, file handles will be recycled, reducing
      performance. This limit should be rarely approached by most SFrame/SArray
      operations. Large SGraphs however may create a large a number of SFrames
      in which case increasing this limit may improve performance (You may
      also need to increase the system file handle limit with "ulimit -n").
      Defaults to 128.

    GRAPHLAB_SFRAME_IO_READ_LOCK
      Whether disk reads should be locked. Almost always necessary for magnetic
      disks for consistent performance. Can be disabled on SSDs. Defaults to
      True.

    GRAPHLAB_SFRAME_DEFAULT_BLOCK_SIZE
      The block size used by the SFrame file format. Increasing this will
      increase throughput of single SArray accesses, but decrease throughput of
      wide SFrame accesses. Defaults to 65536 bytes.

    ----------
    name: A string referring to runtime configuration variable.

    value: The value to set the variable to.

    Returns
    -------
    Nothing

    Raises
    ------
    A RuntimeError if the key does not exist, or if the value cannot be
    changed to the requested value.

    """
    unity = _glconnect.get_unity()
    ret = unity.set_global(name, value)
    if ret != "":
        raise RuntimeError(ret);

GLOB_RE = _re.compile("""[*?]""")
def split_path_elements(url):
    parts = _os.path.split(url)
    m = GLOB_RE.search(parts[-1])
    if m:
        return (parts[0], parts[1])
    else:
        return (url, "")

def validate_feature_types(dataset, features, valid_feature_types):
    if features is not None:
        if not hasattr(features, '__iter__'):
            raise TypeError("Input 'features' must be an iterable type.")

        if not all([isinstance(x, str) for x in features]):
            raise TypeError("Input 'features' must contain only strings.")

    ## Extract the features and labels
    if features is None:
        features = dataset.column_names()

    col_type_map = {
        col_name: col_type for (col_name, col_type) in
        zip(dataset.column_names(), dataset.column_types()) }

    valid_features = []
    for col_name in features:
        if col_type_map.get(col_name) in valid_feature_types:
            valid_features.append(col_name)

    if len(valid_features) == 0:
        if features is not None:
            raise TypeError("No valid feature types specified")
        else:
            raise TypeError("The dataset does not contain any valid feature types")

    return features


def crossproduct(d):
    """
    Create an SFrame containing the crossproduct of all provided options.

    Parameters
    ----------
    d : dict
        Each key is the name of an option, and each value is a list
        of the possible values for that option.

    Returns
    -------
    out : SFrame
        There will be a column for each key in the provided dictionary,
        and a row for each unique combination of all values.

    Example
    -------
    settings = {'argument_1':[0, 1],
                'argument_2':['a', 'b', 'c']}
    print crossproduct(settings)
    +------------+------------+
    | argument_2 | argument_1 |
    +------------+------------+
    |     a      |     0      |
    |     a      |     1      |
    |     b      |     0      |
    |     b      |     1      |
    |     c      |     0      |
    |     c      |     1      |
    +------------+------------+
    [6 rows x 2 columns]
    """

    _mt._get_metric_tracker().track('util.crossproduct')
    from graphlab import SArray
    d = [zip(d.keys(), x) for x in _itertools.product(*d.values())]
    sa = [{k:v for (k,v) in x} for x in d]
    return SArray(sa).unpack(column_name_prefix='')


class Singleton:
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

from distutils.version import StrictVersion
import logging


def __get_version(version):
    if 'dev' in str(version):
        version = version[:version.find('.dev')]

    return StrictVersion(version)


PANDAS_MIN_VERSION = '0.13.0'
try:
    import pandas
    if __get_version(pandas.__version__) < StrictVersion(PANDAS_MIN_VERSION):
        HAS_PANDAS = False
        logging.warn('Pandas version {} is not supported. Minimum required version: {}. '
                     'Pandas support will be disabled.'.format(pandas.__version__, PANDAS_MIN_VERSION))
    else:
        HAS_PANDAS = True
except:
    HAS_PANDAS = False
    import pandas_mock as pandas


NUMPY_MIN_VERSION = '1.8.0'
try:
    import numpy

    if __get_version(numpy.__version__) < StrictVersion(NUMPY_MIN_VERSION):
        HAS_NUMPY = False
        logging.warn('Numpy version {} is not supported. Minimum required version: {}. '
                     'Numpy support will be disabled.'.format(numpy.__version__, NUMPY_MIN_VERSION))
    else:
        HAS_NUMPY = True

except:
    HAS_NUMPY = False
    import numpy_mock as numpy


try:
    import matplotlib

    HAS_MATPLOTLIB = True

except:
    HAS_MATPLOTLIB = False
    import matplotlib_mock as matplotlib


try:
    from xpatterns.analytics import dataframeplus
    HAS_DATAFRAME_PLUS = True

except:
    HAS_DATAFRAME_PLUS = False
    import dataframeplus_mock as dataframeplus

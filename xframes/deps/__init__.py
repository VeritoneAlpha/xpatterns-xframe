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

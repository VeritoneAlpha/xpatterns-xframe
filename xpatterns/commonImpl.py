from xpatterns.util import Singleton
from pyspark import SparkConf, SparkContext, SQLContext
import atexit

CLUSTER_URL = 'local'
APP_NAME = 'xFrame'

@Singleton
class CommonSparkContext:
    def __init__(self):
        conf = SparkConf().setMaster(CLUSTER_URL).setAppName(APP_NAME)
        self.sc = SparkContext(conf=conf)
        self.sqlc = SQLContext(self.sc)
        atexit.register(self.close_context)

    def close_context(self):
        if self.sc:
            self.sc.stop()
            self.sc = None
        if self.sqlc:
            self.sqlc.stop()
            self.sqlc = None


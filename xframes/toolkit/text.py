from pyspark.mllib.feature import Word2Vec

from xframes.spark_context import CommonSparkContext

class TextModel(object):
    def __init__(self, model):
        self.model = model

    def find_associations(self, word, num=10):
        return self.model.findSynonyms(word, num)

class TextBuilder(object):
    def __init__(self, corpus, vector_size=100, seed=42):
        self.corpus = corpus
        self.vector_size = vector_size
        self.seed = seed

    def train(self):
        sc = CommonSparkContext.Instance().sc()
        rdd = self.corpus.to_spark_rdd()
        model = Word2Vec().setVectorSize(self.vector_size).setSeed(self.seed).fit(rdd)
        return TextModel(model)

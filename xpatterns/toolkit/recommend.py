from abc import ABCMeta

from xpatterns.toolkit.model import Model, ModelBuilder

from pyspark.mllib.recommendation import MatrixFactorizationModel, ALS, Rating

# Models
class RecommenderModel(Model):
    __metaclass__ = ABCMeta

class MatrixFactorizationModel(RecommenderModel):
    def __init__(self, model):
        self.model = model

    def predict(self, user, product):
        return self.model.predict(user, product)

    def predict_all(self, user_product):
        return self.model.predictAll(user_product)

    def recommend(self, user, product):
        return self.model._java_model.recommendProducts(user, product)

    def product_features(self):
        return self.model.productFeatures()

    def user_features(self):
        return self.model.userFeatures()

# Builders
class RecommenderBuilder(ModelBuilder):
    __metaclass__ = ABCMeta


class ALSBuilder(RecommenderBuilder):
    def __init__(self, xframe, user_col, product_col, rating_col):
        def create_rating(row):
            return [row[user_col], row[product_col], row[rating_col]]
        self.ratings = xframe.apply(create_rating)

    def train(self, rank, **kwargs):
        model = ALS.train(self.ratings.__impl__.rdd.rdd, rank, **kwargs)
        return MatrixFactorizationModel(model)

    def trainImplicit(self, rank, **kwargs):
        model = ALS.train(rank, **kwargs)
        return MatrixFactorizationModel(model)
        


def create(data, user_id, item_id, target):
    return ALSBuilder(data, user_id, item_id, target).train(1)

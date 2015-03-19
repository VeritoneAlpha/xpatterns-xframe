from abc import ABCMeta

from xpatterns.toolkit.model import Model, ModelBuilder
from xpatterns.xarray_impl import XArrayImpl
from xpatterns.xarray import XArray
from xpatterns.xframe_impl import XFrameImpl
from xpatterns.xframe import XFrame
from xpatterns.xrdd import XRdd

from pyspark.mllib.recommendation import MatrixFactorizationModel, ALS, Rating

# Models
class RecommenderModel(Model):
    __metaclass__ = ABCMeta

from pyspark import RDD
class MatrixFactorizationModel(RecommenderModel):
    def __init__(self, model, builder):
        self.model = model
        self.user_col = builder.user_col
        self.product_col = builder.product_col
        self.rating_col = builder.rating_col
        self.products = builder.products
        self.users = builder.users
        self.ratings = builder.raw_ratings

    def predict(self, user, product):
        res = self.model.predict(user, product)
        return res

    def predict_all(self, user):
        # build rdd to pass to predictAll
        user_product = XFrame()
        user_product[self.product_col] = self.products
        user_product[self.user_col] = user
        user_product.swap_columns(self.product_col, self.user_col)
        rdd = user_product.__impl__.rdd.rdd
        res = self.model.predictAll(rdd)
        res = res.map(lambda rating: [rating.user, rating.product, rating.rating])
        res = XRdd(res)
        col_names = [self.user_col, self.product_col, self.rating_col]
        user_type = self.users.dtype()
        product_type = self.products.dtype()
        col_types = [user_type, product_type, float]
        return XFrame(_impl=XFrameImpl(rdd=res, col_names=col_names, column_types=col_types))

    def recommend(self, user, product):
        return self.model._java_model.recommendProducts(user, product)

    def recommend_top_k(self, user, k=10):
        predictions = self.predict_all(user)
        # filter out the movies that a user has rated
        rated_products = self.ratings.filterby(user, self.user_col)[self.product_col]
        predictions = predictions.filterby(rated_products, self.product_col, exclude=True)
        topk = predictions.topk(self.rating_col, k)
        return topk

    def product_features(self):
        return XArray(_impl=XArrayImpl(self.model.productFeatures(), list))

    def user_features(self):
        return XArray(_impl=XArrayImpl(self.model.userFeatures(), list))

# Builders
class RecommenderBuilder(ModelBuilder):
    __metaclass__ = ABCMeta


class ALSBuilder(RecommenderBuilder):
    def __init__(self, ratings, user_col, product_col, rating_col):
        def create_rating(row):
            return [row[user_col], row[product_col], row[rating_col]]
        self.raw_ratings = ratings
        self.ratings = ratings.apply(create_rating)
        self.products = self.ratings.apply(lambda item: item[1]).unique()
        self.users = self.ratings.apply(lambda item: item[0]).unique()
        self.user_col = user_col
        self.product_col = product_col
        self.rating_col = rating_col

    def train(self, rank, **kwargs):
        model = ALS.train(self.ratings.__impl__.rdd.rdd, rank, **kwargs)
        return MatrixFactorizationModel(model, self)

    def trainImplicit(self, rank, **kwargs):
        model = ALS.train(rank, **kwargs)
        return MatrixFactorizationModel(model)
        


def create(data, user_id, item_id, target, rank=50, iterations=10, lambda_=0.01, **kwargs):
    return ALSBuilder(data, user_id, item_id, target) \
        .train(rank, iterations=iterations, lambda_=lambda_, **kwargs)

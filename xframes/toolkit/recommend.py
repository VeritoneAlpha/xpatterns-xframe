from abc import ABCMeta, abstractmethod

import os
import pickle

from xframes.spark_context import CommonSparkContext
from xframes.toolkit.model import Model, ModelBuilder
from xframes import XArray, XFrame
from xframes.xarray_impl import XArrayImpl
from xframes.xframe_impl import XFrameImpl
from xframes.util import delete_file_or_dir

from pyspark import RDD
from pyspark.mllib import recommendation
from pyspark.mllib.recommendation import ALS, Rating

##
## TODO
## write metadata in sub-directory
# add __repr__ and __str__ functions for display


# Models
class RecommenderModel(Model):
    __metaclass__ = ABCMeta

    def __init__(self, model, ratings, user_col, item_col, rating_col):
        self.model = model
        self.ratings = ratings
        self.user_col = user_col
        self.item_col = item_col
        self.rating_col = rating_col

    def __repr__(self):
        res = '{!r}\n'.format(self.model)
        res += 'user_col: {}\n'.format(self.user_col)
        res += 'item_col: {}\n'.format(self.item_col)
        res += 'rating_col: {}'.format(self.rating_col)
        return res

    @staticmethod
    def _file_paths(path):
        """
        Return the file paths for model, ratings, and metadata.
        """
        model_path = os.path.join(path, 'model')
        ratings_path = os.path.join(path, 'ratings')
        metadata_path = os.path.join(path, '_metadata')
        return (model_path, ratings_path, metadata_path)

class MatrixFactorizationModel(RecommenderModel):
    """
    Recommender model.
    """

    def __init__(self, model, ratings, user_col, item_col, rating_col):
        super(MatrixFactorizationModel, self).  \
            __init__(model, ratings, user_col, item_col, rating_col)
        self.users = self.ratings.apply(lambda row: row[user_col]).unique()
        self.items = self.ratings.apply(lambda row: row[item_col]).unique()

    # TODO - when there is a second derived type, 
    #  see what we can move to the base class.
    def predict(self, user, item):
        """
        Predict the rating given by a user to an item.

        Parameters
        ----------
        user : int
            The user to predict.
        
        item : int
            The item to rate.

        Returns
        -------
        out : float
            The predicted rating.
        """

        res = self.model.predict(user, item)
        return res

    def predict_all(self, user):
        """
        Predict ratings for all items.

        Parameters
        ----------
        user : int
            The user to make predictions for.

        Returns
        -------
        out : XFrame
            Each row of the frame consists of a user id, an item id, and a predicted rating.
        """

        # build rdd to pass to predictAll
        user_item = XFrame()
        user_item[self.item_col] = self.items
        user_item[self.user_col] = user
        user_item.swap_columns(self.item_col, self.user_col)
        rdd = user_item.to_rdd()
        res = self.model.predictAll(rdd)
        res = res.map(lambda rating: (rating.user, rating.product, rating.rating))
        col_names = [self.user_col, self.item_col, self.rating_col]
        user_type = self.users.dtype()
        item_type = self.items.dtype()
        col_types = [user_type, item_type, float]
        return XFrame.from_rdd(res, column_names=col_names, column_types=col_types)

    def recommend(self, user, item):
        return self.model._java_model.recommendProducts(user, item)

    def recommend_top_k(self, user, k=10):
        """
        Recommend some items for a user.

        Parameters
        ----------
        user : int
            The user to make recommendations for.

        Returns
        -------
        out : XFrame
            A XFrame containing the highest predictions for the user.
            The items that the user has explicitly rated are excluded.
        """
        predictions = self.predict_all(user)
        # filter out the movies that a user has rated
        rated_items = self.ratings.filterby(user, self.user_col)[self.item_col]
        predictions = predictions.filterby(rated_items, self.item_col, exclude=True)
        topk = predictions.topk(self.rating_col, k)
        return topk

    def item_features(self):
        """
        The item features.

        Underlying model parameters.
        """
        return XArray.from_rdd(self.model.productFeatures(), list)

    def user_features(self):
        """
        The user features.

        Underlying model parameters.
        """
        return XArray.from_rdd(self.model.userFeatures(), list)

    def save(self, path):
        """
        Save a model.

        The model can be saved, then reloaded later to provide recommendations.

        Parameters
        ----------
        path : str
            The path where the model will be saved.
            This should refer to a file, not to a directory.
            Three items will be stored here: the underlying model parameters, the original ratings,
            and the column names.  These are stored with suffix '.model', '.ratings', and
            '.metadata'.
        """
        sc = CommonSparkContext.Instance().sc()
        delete_file_or_dir(path)
        os.makedirs(path)
        model_path, ratings_path, metadata_path = self._file_paths(path)
        # save model
        self.model.save(sc, model_path)
        # save ratings
        self.ratings.save(ratings_path)
        # save metadata
        metadata = [self.user_col, self.item_col, self.rating_col]
        with open(metadata_path, 'w') as f:
            pickle.dump(metadata, f)

    @classmethod
    def load(cls, path):
        """
        Load a model that was saved previously.

        Parameters
        ----------
        path : str
            The path where the model files are stored.
            This is the same path that was passed to ``save``.
            There are three files/directories based on this path, with
            extensions '.model', '.ratings', and '.metadata'.

        Returns
        -------
        out : MatrixFactorizationModel
            A model that can be used to predict ratings.
        """
        sc = CommonSparkContext.Instance().sc()
        model_path, ratings_path, metadata_path = cls._file_paths(path)
        # load model
        model = recommendation.MatrixFactorizationModel.load(sc, model_path)
        # load ratings
        ratings = XFrame.load(ratings_path)
        # load metadata
        with open(metadata_path) as f:
            user_col, item_col, rating_col = pickle.load(f)

        return cls(model, ratings, user_col, item_col, rating_col)


# Builders
class RecommenderBuilder(ModelBuilder):
    __metaclass__ = ABCMeta

    def __init__(self, ratings, user_col, item_col, rating_col):
        self.ratings = ratings
        self.user_col = user_col
        self.item_col = item_col
        self.rating_col = rating_col

    def _prepare_ratings(self):
        user_col = self.user_col
        item_col = self.item_col
        rating_col = self.rating_col
        def create_rating(row):
            return [row[user_col], row[item_col], row[rating_col]]
        ratings = self.ratings.apply(create_rating)
        return ratings                             
                                 
    @abstractmethod
    def train(self):
        pass

class ALSBuilder(RecommenderBuilder):
    def __init__(self, ratings, user_col, item_col, rating_col):
        """
        Create an ALSBuilder.

        The builder can be used to train a model.

        Parameters
        ----------
        ratings : XFrame
            A table containing the user ratings.  This table must contin three columns corresponding to the
            users, the items, and the ratings.  The table may contain other columns as well: these are not 
            used.

        user_col : string
            The column name of the users.

        item_col : string
            The column name of the items.

        rating_col : string
            The column name of the ratings.  This must be a number.

            
        """
        super(ALSBuilder, self).  \
            __init__(ratings, user_col, item_col, rating_col)

    def train(self, rank, iterations=10, lambda_=0.01, seed=0, **kwargs):
        """
        Train the model.

        Parameters
        ----------
        rank : int
            The number of factors in the underlying model.  Generally, larger numbers of factors
            lead to better models, but increase the memory required.  A rank in the range of 10 to 200
            is usually reasonable.

        iterations : int, optional
            The number of iterations to perform.  With each iteration, the model improves.  ALS
            typically converges quickly, so a value of 10 is recommended.

        lambda : float, optional
            This parameter controls regularization, which controls overfitting.  The higher the value of
            lambda applies more regularization.  The appropriate value here depends on the problem, and needs
            to be tuned by train/test techniques, which measure overfitting.

        Returns
        -------
        out: : model
            A RecommenderModel.  This can be used to make predidictions on how a user would rate an item.
        """

        ratings = self._prepare_ratings()
        model = ALS.train(ratings.to_rdd(),
                          rank, 
                          iterations=iterations, 
                          lambda_=lambda_, 
                          seed=seed, 
                          **kwargs)
        return MatrixFactorizationModel(model, self.ratings, self.user_col, self.item_col, self.rating_col)

    def train_implicit(self, rank, seed=0, iterations=50, lambda_=0.01, **kwargs):
        """
        Train the model using implicit ratings.

        Parameters
        ----------
        rank : int
            The number of factors in the underlying model.  Generally, larger numbers of factors
            lead to better models, but increase the memory required.  
            A rank in the range of 10 to 200 is usually reasonable.

        iterations : int, optional
            The number of iterations to perform.  With each iteration, the model improves.  ALS
            typically converges quickly, so a value of 10 is recommended.

        lambda : float, optional
            This parameter controls regularization, which controls overfitting.  
            The higher the value of lambda applies more regularization.  
            The appropriate value here depends on the problem, and needs
            to be tuned by train/test techniques, which measure overfitting.

        Returns
        -------
        out: : model
            A RecommenderModel.  This can be used to make predidictions on how a 
            user would rate an item.
        """

        ratings = self._prepare_ratings()
        model = ALS.trainImplicit(ratings.to_rdd(),
                          rank, 
                          iterations=iterations, 
                          lambda_=lambda_, 
                          seed=seed, 
                          **kwargs)
        return MatrixFactorizationModel(model, self.ratings, self.user_col, self.item_col, self.rating_col)


def create(data, user_col, item_col, rating_col, 
           recommender_type='ALS', rank=50, 
           iterations=10, lambda_=0.01, seed=0, **kwargs):
    """
    Create a recommendation model.

    Parameters
    ----------

    data : XFrame
        A table containing the user ratings.  
        This table must contin three columns corresponding to the
        users, the items, and the ratings.  
        The table may contain other columns as well: these are not used.

    user_col : string
        The column name of the users.

    item_col : string
        The column name of the items.

    rating_col : string
        The column name of the ratings.  This must be a number.

    recommender_type : string, optional
        The type of recommender.  Optons are:
        * ALS
        * ALS-implicit

    rank : int, optional
        See ``ALSBuilder.train``

    iterations : int, optional
        See ``ALSBuilder.train``

    lambda_ : float, optional
        See ``ALSBuilder.train``

    other : various, optional
        See optional arguments to pyspark.mllib.recommendations.train.
    """

    if recommender_type == 'ALS':
        return ALSBuilder(data, user_col, item_col, rating_col) \
            .train(rank=rank, iterations=iterations, lambda_=lambda_, seed=seed, **kwargs)
    if recommender_type == 'ALS-implicit':
        return ALSBuilder(data, user_col, item_col, rating_col) \
            .train_implicit(rank=rank, iterations=iterations, lambda_=lambda_, seed=seed, **kwargs)
    raise ValueError('recommender type is not recognized')

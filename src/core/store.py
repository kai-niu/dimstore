import dill
import json
import copy
from src.providers.readers import Reader_Registration
from src.providers.writers import Writer_Registration
from src.providers.serializers import Serializer_Registration
from src.providers.deserializers import Deserializer_Registration
from src.providers.bookkeeper import Book_Keeper_Factory
from src.core.feature import Feature


"""
    Feature Store class store provide API:
    1. register new features
    2. checkout existing features
    3. manage features
"""

class Store():

    """
    "   init the store and configure feature store 
    """
    def __init__(self, config_file_path, verbose=True):
        self.config = None

        # read store configuration
        with open(config_file_path, "r") as config_file:
            self.config = json.loads(config_file.read())

        # get provider registration
        self.registered_readers = Reader_Registration().providers
        self.registered_writers = Writer_Registration().providers
        self.registered_serializers = Serializer_Registration().providers
        self.registered_deserializers = Deserializer_Registration().providers

        # init console
        book_keeper_factory = Book_Keeper_Factory(self.config)
        self.book_keeper = book_keeper_factory.get_book_keeper() 

        # dump configuration
        # !todo: beautify the output
        if verbose:
            print('== Store Initialized: ==')
            print(json.dumps(self.config, indent=2, sort_keys=False))

    """
    "  register the feature to store
    """
    def register(self, feature, pipeline, **kwargs):
        # serialzie pipeline
        serializer = self.registered_serializers[feature.serializer]
        dumps = serializer(pipeline, self.config, **kwargs)

        # persist the serialized pipeline
        writer = self.registered_writers[feature.writer]
        writer(feature, dumps, self.config, **kwargs)

        # add new registered feature into store catelog
        self.book_keeper.register(feature)


    """
        retrieve feature from store
    """
    def checkout(self, feature_id, params, **kwargs):
        # read in the feature object 
        feature = self.book_keeper.lookup(feature_id)
        if feature != None:
            # retrieve the serialized pipeline
            reader = self.registered_readers[feature.reader]
            dumps = reader(feature_id, self.config, **kwargs)

            # deserialize the pipeline
            deserializer =  self.registered_deserializers[feature.deserializer]
            pipeline = deserializer(dumps, self.config, **kwargs)

            return pipeline(params)
        else:
            return None

    """
        list all available features
    """
    def catalog(self, **kwargs):
        feature_list =  self.book_keeper.all_features(**kwargs)
        print('== Feature Catalog ==')
        for _,v in feature_list.items():
            print('%s \t %s'%(v.name, v.uid))


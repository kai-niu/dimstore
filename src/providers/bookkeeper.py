"""
"  default management console, keep records of all features in store
"""
import os
import pickle as pl
from src.core.feature import Feature
from src.utility.file_functions import file_exist, read_binary_file, write_binary_file


class Default_Book_Keeper():

    def __init__(self, config):
        self.config = config
        self.path = "%s/%s"%(config['root_dir'], config['book_keeper']['params']['folder_name'])
        self.filename = self.config['book_keeper']['params']['file_name']

    def register(self, feature):
        # check if book catalog file exist and
        # read in the catalog dictionary 
        if not file_exist(self.path, self.filename):
            catalog = {}
            catalog[str(feature.uid)] = feature
            
        else:
            bytes_obj = read_binary_file(self.path, self.filename)
            catalog = pl.loads(bytes_obj)
            catalog[str(feature.uid)] = feature

        # write back catalog bytes object
        dumps = pl.dumps(catalog)
        write_binary_file(self.path, self.filename, dumps)

    def lookup(self, uid):
        # look up the feature object by uid
        if file_exist(self.path, self.filename):
            bytes_obj = read_binary_file(self.path, self.filename)
            catalog = pl.loads(bytes_obj)
            if uid in catalog:
                return catalog[uid]
            print("uid in catalog", uid in catalog)
        return None

    def all_features(self, **kwargs):
        if not file_exist(self.path, self.filename):
            catalog = {}
            
        else:
            bytes_obj = read_binary_file(self.path, self.filename)
            catalog = pl.loads(bytes_obj)

        return catalog




class Book_Keeper_Factory():

    def __init__(self, config):
        self.config = config

    # book keeper factory
    def get_book_keeper(self):
        if self.config['book_keeper']['type'] == 'default':
            bookkeeper = Default_Book_Keeper(self.config)
            return bookkeeper

"""
"  default flat file meta data manager, keep records of all features in store
"""
import os
import pickle as pl
from src.providers.meta_manager.meta_manager_base import MetaManagerBase
from src.utility.file_functions import file_exist, read_binary_file, write_binary_file


class FlatFileMetaManager(MetaManagerBase):

    def __init__(self, config):
        self.config = config
        self.path = "%s/%s"%(config['default']['root_dir'], config['default']['folder_name'])
        self.filename = self.config['default']['file_name']

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
        return None

    def list_features(self, **kwargs):
        if not file_exist(self.path, self.filename):
            catalog = {}  
        else:
            bytes_obj = read_binary_file(self.path, self.filename)
            catalog = pl.loads(bytes_obj)

        return catalog

    def inspect_feature(self, uid, **kwargs):
        if not file_exist(self.path, self.filename):
            catalog = {}
        else:
            bytes_obj = read_binary_file(self.path, self.filename)
            catalog = pl.loads(bytes_obj)

        if uid in catalog:
            return catalog[uid]
        else:
            return None

    def remove_feature(self, uid, **kwargs):

        # check if book catalog file exist and
        # read in the catalog dictionary 
        if not file_exist(self.path, self.filename):
            catalog = {}        
        else:
            bytes_obj = read_binary_file(self.path, self.filename)
            catalog = pl.loads(bytes_obj)
            if uid in catalog: del catalog[uid]

        # write back catalog bytes object
        dumps = pl.dumps(catalog)
        write_binary_file(self.path, self.filename, dumps)



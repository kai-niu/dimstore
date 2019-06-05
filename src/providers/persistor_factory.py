"""
    persistor factory
"""
from src.providers.flatfile_persistor import Flat_File_Persistor

class Persistor_Factory():

    def __init__(self, config):
        self.config = config

    # fabricate persistor
    def get_persistor(self, type):
        if type == 'default':
            return Flat_File_Persistor(self.config['persistor_providers'])

    # return supported persistor info
    def info(self):
        return ['flat file persistor (default)']
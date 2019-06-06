"""
    persistor factory
"""
from src.providers.persistor.flatfile_persistor import FlatFilePersistor

class PersistorFactory():

    def __init__(self, config):
        self.config = config

    # fabricate persistor
    def get_persistor(self, type):
        if type == 'default':
            return FlatFilePersistor(self.config['persistor_providers'])

    # return supported persistor info
    def info(self):
        return ['flat file persistor (default)']
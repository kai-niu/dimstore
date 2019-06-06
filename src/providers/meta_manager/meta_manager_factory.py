"""
    meta manager factory
"""

from src.providers.meta_manager.flatfile_meta_manager import FlatFileMetaManager

class MetaManagerFactory():

    def __init__(self, config):
        self.config = config

    # meta manager factory
    def get_meta_manager(self):
        if self.config['meta_manager'] == 'default':
            return FlatFileMetaManager(self.config['meta_manager_providers'])

    # return supported meta manager info
    def info(self):
        return ['flat file meta manager (default)']
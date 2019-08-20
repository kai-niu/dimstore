"""
    meta manager factory
"""

from nebula.providers.meta_manager.flatfile_meta_manager import FlatFileMetaManager

class MetaManagerFactory():

    def __init__(self, config):
        self.config = config

    # meta manager factory
    def get_meta_manager(self):
        if self.config['meta_manager'] == 'flat_file_meta_manager':
            return FlatFileMetaManager(self.config['meta_manager_providers']['flat_file_meta_manager'])

    # return supported meta manager info
    def info(self):
        return ['flat_file_meta_manager: flat file meta manager.']
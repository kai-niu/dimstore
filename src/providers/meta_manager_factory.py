"""
    meta manager factory
"""

from src.providers.flatfile_meta_manager import Flat_File_Meta_Manager

class Meta_Manager_Factory():

    def __init__(self, config):
        self.config = config

    # meta manager factory
    def get_meta_manager(self):
        if self.config['meta_manager'] == 'default':
            return Flat_File_Meta_Manager(self.config['meta_manager_providers'])

    # return supported meta manager info
    def info(self):
        return ['flat file meta manager (default)']
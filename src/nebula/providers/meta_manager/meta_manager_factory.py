"""
    meta manager factory
"""

from nebula.providers.meta_manager.flatfile_meta_manager import FlatFileMetaManager
from nebula.providers.meta_manager.ibm_wkc_meta_manager import WastonKnowledgeCatalogMetaManager

class MetaManagerFactory():

    def __init__(self, config):
        self.config = config

    # meta manager factory
    def get_meta_manager(self):
        if self.config['meta_manager'] == 'flat_file':
            return FlatFileMetaManager(self.config['meta_manager_providers']['flat_file'])
        elif self.config['meta_manager'] == 'ibm_waston_knowledge_catalog':
            return WastonKnowledgeCatalogMetaManager(self.config['meta_manager_providers']['ibm_waston_knowledge_catalog'])
        else:
            raise Exception('> meta manager provider: %s is not supported' % (self.config['meta_manager']))

    # return supported meta manager info
    def info(self):
        return ['flat_file: flat file meta manager.','ibm_waston_knowledge_catalog: waston knowledge catalog meta manager.']
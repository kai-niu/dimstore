"""
    persistor factory
"""
from dimstore.providers.persistor.flatfile_persistor import FlatFilePersistor
from dimstore.providers.persistor.ibm_object_storage_persistor import IBMObjectStoragePersistor
from dimstore.providers.persistor.waston_knowledge_catalog_persistor import WastonKnowlegeCatalogPersistor

class PersistorFactory():

    def __init__(self, config):
        self.config = config

    # fabricate persistor
    def get_persistor(self, persistor):
        if persistor == 'flat_file':
            return FlatFilePersistor(self.config['persistor_providers']['flat_file'])
        elif persistor == 'ibm_object_storage':
            return IBMObjectStoragePersistor(self.config['persistor_providers']['ibm_object_storage'])
        elif persistor == 'ibm_waston_knowledge_catalog':
            return WastonKnowlegeCatalogPersistor(self.config['persistor_providers']['ibm_waston_knowledge_catalog'])
        else:
            raise Exception('> persistor provider: %s is not supported' % (persistor))

    # return supported persistor info
    def info(self):
        return ['flat_file: flat file persistor.', 
                'ibm_object_storage: IBM object storage persistor.',
                'ibm_waston_knowledge_catalog: IBM Waston Knowledge Catalog persistor.']
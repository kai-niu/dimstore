"""
    persistor factory
"""
from nebula.providers.persistor.flatfile_persistor import FlatFilePersistor
from nebula.providers.persistor.ibm_object_storage_persistor import IBMObjectStoragePersistor
from nebula.providers.persistor.waston_knowledge_catalog_persistor import WastonKnowlegeCatalogPersistor

class PersistorFactory():

    def __init__(self, config):
        self.config = config

    # fabricate persistor
    def get_persistor(self, type):
        if type == 'flat_file_storage':
            return FlatFilePersistor(self.config['persistor_providers']['flat_file_storage'])
        elif type == 'ibm_object_storage':
            return IBMObjectStoragePersistor(self.config['persistor_providers']['ibm_object_storage'])
        elif type == 'ibm_waston_knowledge_catalog':
            return WastonKnowlegeCatalogPersistor(self.config['persistor_providers']['ibm_waston_knowledge_catalog'])

    # return supported persistor info
    def info(self):
        return ['flat_file_storage: flat file persistor.', 
                'ibm_object_storage: IBM object storage persistor.',
                'ibm_wkc_storage: IBM Waston Knowledge Catalog persistor.']
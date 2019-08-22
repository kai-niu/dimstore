"""
"  define the feature metadata keys 
"""
from nebula.core.feature_metadata import FeatureMetaData

class MetadataBuilder():
    def __init__(self, store_proxy):
        self.name = 'name'
        self.index = 'index'
        self.namespace = 'namespace'
        self.author = 'author'
        self.dataset = 'dataset'
        self.params = 'params'
        self.comment = 'comment'
        self.create_date = 'create_date'
        self.persistor = 'persistor'
        self.serializer = 'serializer'
        if store_proxy == None:
            raise Exception('> Metadatabuilder __init__: store_proxy object can not be none.')
        else:
            self.store_proxy = store_proxy
    
    """
    "
    " return a list of metadata keys describes feature object
    "
    """
    @staticmethod
    def keys():
        """
        @param: empty parameter inteneded
        return a list of keys in string
        """
        return ['name', 
                'index',
                'namespace',
                'author',
                'dataset',
                'params',
                'comment',
                'create_date',
                'persistor',
                'serializer']

    """
    "
    " build the feature metadata object
    "
    """
    def feature(self, name, index, namespace=None):
        """
        @param::name: feature name in string
        @param::index: feature index column in string
        @param::namespace: namespace in string
        return feature metadata object
        """
        # check edge case
        if name == None or name.strip() == '':
            raise Exception('> MetadataBuilder build method: feature name can not be None or empty!')
        if index == None or name.strip() == '':
            raise Exception('> MetadataBuilder build method: feature index name can not be None or empty!')
        meta_data = FeatureMetaData(name, index, namespace=namespace)
        meta_data.persistor = self.store_proxy.config['default_persistor']
        meta_data.serializer = self.store_proxy.config['default_serializer']
        return meta_data
        


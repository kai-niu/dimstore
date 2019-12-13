"""
"  define the feature metadata keys 
"""
from nebula.core.feature_metadata import FeatureMetaData
import re

class MetadataBuilder():
    def __init__(self, store_proxy):
        self.name = 'name'
        self.index = 'index'
        self.namespace = 'namespace'
        self.author = 'author'
        self.dataset = 'dataset'
        self.params = 'params'
        self.output = 'output'
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
                'output',
                'comment',
                'create_date',
                'persistor',
                'serializer']

    """
    "
    " build the feature metadata object
    "
    """
    def feature(self, name, index, output, namespace=None):
        """
        @param::name: feature name in string
        @param::index: feature index column in string
        @param::namespace: namespace in string
        return feature metadata object
        """
        # assign default namespace value
        if namespace ==  None:
            namespace = 'default'
        if namespace != None and namespace.strip() == '':
            namespace = 'default'

        # assign default value
        meta_data = FeatureMetaData(name, index, output, namespace=namespace)
        meta_data.persistor = self.store_proxy.config['default_persistor']
        meta_data.serializer = self.store_proxy.config['default_serializer']
        if self.sanity_check(meta_data):
            return meta_data
        else:
            raise ValueError('> MetadataBuilder.feature(): feature object sanity check failed.')

    """
    "
    " sanity check feature name
    "
    """
    @classmethod
    def sanity_check(cls,feature):
        if not isinstance(feature, FeatureMetaData):
            raise TypeError ('> MetadataBuilder.sanity_check(): feature object is not instance of FeatureMetaData class.')
        if feature.name == None:
            raise ValueError('> MetadataBuilder.sanity_check(): feature name can not be None.')
        if re.match(r"^[\w_]+$", feature.name) is None:
            raise ValueError('> MetadataBuilder.sanity_check(): feature name can only contain alphnumeric and "_" characters.')
        if feature.index == None or feature.index == '':
            raise ValueError('> MetadataBuilder.sanity_check(): feature index can not be None or empty!')
        if feature.output == None or feature.output.strip() == '':
            raise ValueError('> MetadataBuilder.sanity_check(): feature output type can not be None or empty!')
        if re.match(r"^[\w.]+$", feature.namespace) is None:
            raise ValueError('> MetadataBuilder.sanity_check(): feature namespace type can only contain alphanumeric and "." charaxters.')
        return True
        

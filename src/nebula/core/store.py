import dill
import json
import copy

from nebula.providers.persistor.persistor_factory import PersistorFactory
from nebula.providers.serializer.serializer_factory import SerializerFactory
from nebula.providers.cache.cache_layer_factory import CacheLayerFactory
from nebula.providers.meta_manager.meta_manager_factory import MetaManagerFactory 
from nebula.core.feature_metadata import FeatureMetaData
from nebula.core.metadata_keys import MetadataKeys
from nebula.utility.file_functions import read_text_file, http_read_file, parse_file_protocol, parse_file_uri


"""
    Feature Store class store provide API:
    1. register new features
    2. checkout existing features
    3. manage features
"""

class Store():

    """
    "   init the store and configure feature store 
    """
    def __init__(self, config_file_path, verbose=True):
        self.config = None
        self.metakeys = MetadataKeys()
        self.verbose = verbose

        # read store configuration
        config_data = self.__fetch_config__(config_file_path)
        if config_data == None:
            raise Exception('Load configuration file failed: %s'%(config_file_path))
        else:
            self.config = json.loads(config_data)

        # init serializer factory
        self.serializer_factory = SerializerFactory(self.config)

        # init cache layer factory
        self.cache_layer_factory = CacheLayerFactory(self.config)

        # init persistor factory
        self.persistor_factory = PersistorFactory(self.config)

        # init console
        self.meta_manager_factory = MetaManagerFactory(self.config)
        self.meta_manager = self.meta_manager_factory.get_meta_manager()

    def __fetch_config__(self, config_file_path):
        config = None
        # parse config file protocol
        protocol = parse_file_protocol(config_file_path)
        # read config file based on the protocol
        if protocol == 'http' or protocol == 'https':
            config = http_read_file(config_file_path)
        elif protocol == 'file':
            dirname, filename = parse_file_uri(config_file_path)
            config = read_text_file(dirname,filename)
        else:
            pass
        return config

    """
    "  return an instance of feature metadata instance
    """
    def build_feature_metadata(self, name, namespace='default'):
        meta_data = FeatureMetaData(name, namespace=namespace)
        meta_data.persistor = self.config['default_persistor']
        meta_data.serializer = self.config['default_serializer']
        return meta_data

    """
    "  register the feature to store
    """
    def register(self, feature, pipeline, **kwargs):
        # serialzie pipeline
        serializer = self.serializer_factory.get_serializer(feature.serializer)
        dumps = serializer.encode(pipeline, **kwargs)

        # persist the serialized pipeline
        persistor = self.persistor_factory.get_persistor(feature.persistor)
        persistor.write(feature, dumps, **kwargs)

        # add new registered feature into store catelog
        self.meta_manager.register(feature)


    """
        retrieve feature from store
    """
    def checkout(self, feature_id, params, **kwargs):
        # read in the feature object 
        feature = self.meta_manager.lookup(feature_id)
        if feature != None:
            # retrieve the serialized pipeline
            persistor = self.persistor_factory.get_persistor(feature.persistor)
            dumps = persistor.read(feature_id, **kwargs)
            # deserialize the pipeline
            serializer = self.serializer_factory.get_serializer(feature.serializer)
            pipeline = serializer.decode(dumps, **kwargs)

            return pipeline(params)
        else:
            return None

    """
        remove feature from store
    """
    def remove(self, feature_id, **kwargs):
        # read in the feature object 
        feature = self.meta_manager.lookup(feature_id)
        if feature != None:
            # retrieve the serialized pipeline
            persistor = self.persistor_factory.get_persistor(feature.persistor)
            persistor.delete(feature_id, **kwargs)
            self.meta_manager.remove_feature(feature_id, **kwargs)

    """
        list all matched features by filter function in given matched namespace
    """
    def list_features(self, namespace='default', **kwargs):
        feature_list =  self.meta_manager.list_features(namespace=namespace, **kwargs)
        print('== Feature Catalog ==')
        for _,v in feature_list.items():
            print('%s \t %s \t %s \t %s'%(v.name, v.namespace, "{:%d, %b %Y}".format(v.create_date), v.author))

    """
    "
    "   list all namespaces defined in feature store
    "
    """
    def list_namespaces(self, **kwargs):
        namespace_list = self.meta_manager.list_namespaces(**kwargs)
        print('== Namespace List ==')
        for ns in namespace_list:
            print(ns)
    
    """
        show feature detail info
    """
    def feature_info(self, name, namespace, **kwargs):
        feature = self.meta_manager.inspect_feature(name)
        if feature != None:
            print('== Feature Detail ==')
            print('%s \t %s \t %s \t %s'%(feature.name, feature.uid, "{:%d, %b %Y}".format(feature.create_date), feature.author))
            print("params: ")
            for p,v in feature.get_value('params').items():
                print(" "*4, "%s: %s"%(p,v))
            print("comments: %s"%(feature.get_value('comment')))

    """
        show store info
    """
    def info(self, **kwargs):
        print("== %s Information ==" % (self.config['store_name']) )
        print("- Meta Data Manager: %s" % (self.config['meta_manager']))
        print("- Supported Meta Data managers: ", self.meta_manager_factory.info())
        print("- Supported Persistors: ", self.persistor_factory.info())
        print("- Supported Serializers: ", self.serializer_factory.info())
        print("- Supported Cache Layers: ", self.cache_layer_factory.info())





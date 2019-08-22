import dill
import json
import copy

from nebula.providers.persistor.persistor_factory import PersistorFactory
from nebula.providers.serializer.serializer_factory import SerializerFactory
from nebula.providers.cache.cache_layer_factory import CacheLayerFactory
from nebula.providers.meta_manager.meta_manager_factory import MetaManagerFactory 
from nebula.providers.output_render.output_render_factory import OutputRenderFactory
from nebula.providers.dataframe_jointer.dataframe_jointer_factory import DataframeJointerFactory
from nebula.core.feature_metadata import FeatureMetaData
from nebula.core.feature_set import FeatureSet
from nebula.core.metadata_builder import MetadataBuilder
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
        self.verbose = verbose
        self.metadata = MetadataBuilder(self)
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

        # init output render
        self.output_render_factory = OutputRenderFactory(self.config)
        self.output_render = self.output_render_factory.get_output_render()

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
        list all matched features by filter function in given matched namespace
    """
    def features(self, namespace='default', match_child=True, **kwargs):
        ufd = self.meta_manager.read(namespace=namespace, match_child=match_child, **kwargs)
        return FeatureSet(self, ufd=ufd)
        

    """
    "
    "   list all namespaces defined in feature store
    "
    """
    def list_namespaces(self, **kwargs):
        namespace_data = self.meta_manager.namespaces(**kwargs)
        self.output_render.namespace_list(namespace_data)
    
  

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
        print("- Supported Output Render Layers: ", self.output_render_factory.info())


    """
    "
    " render the feature list to output, intended to be called by FeatureSet object
    " through store proxy object.
    "
    """
    def list_features(self, feature_list):
        self.output_render.feature_list(feature_list)


    """
    "
    " build features into dataframe, intended to be called by FeatureSet object
    " through store proxy object.
    "
    """
    def build(self, ufd, dataframe='pyspark', verbose=True, **kwargs):
        """
        @param::ufd: the {uid:feature} dictioary
        @param::dataframe: the dataframe type in string
        @param::verbose: toggle log information
        @param::kwargs: the keyworded parameters
        return the dataframe built from feature list
        """
        output_df = None
        index1 = None
        index2 = None
        if ufd != None or len(ufd) > 0:
            if verbose:
                print('> task: build %s dataframe from %d features ...'%(dataframe, len(ufd)))         
            for _,feature in ufd.items():

                df = self.checkout(feature, verbose=verbose, **kwargs)
                if output_df == None:
                    output_df = df
                    index1 = feature.index
                else:
                    index2 = feature.index
                    if dataframe == 'pandas':
                        output_df = DataframJoiner.pandas_joiner(output_df,df,index1,index2)
                    else:
                        output_df = DataframJoiner.pyspark_joiner(output_df,df,index1,index2)                  

        return output_df

    """
    "
    " check out feature from persistence layer
    "
    """
    def checkout(self, feature, verbose=True, **kwargs):
        """
        @param::feature: the feature metadata object
        @param::verbose: boolean value toggles log info output
        @param::kwargs: the keyed parameter list
        return the feature extracted by executing the pipeline
        """
        if feature != None:
            # retrieve the serialized pipeline
            persistor = self.persistor_factory.get_persistor(feature.persistor)
            dumps = persistor.read(feature.uid, **kwargs)
            # deserialize the pipeline
            serializer = self.serializer_factory.get_serializer(feature.serializer)
            pipeline = serializer.decode(dumps, **kwargs)
            # execute the pipeline
            qualify_name = '.'.join([feature.namespace,feature.name])
            params = {}
            if qualify_name in kwargs:
                params = kwargs[qualify_name]
                if verbose:
                    print('> info: checkout "%s" feature with params: %s ...'%(feature.name, params))
            else:
                if verbose:
                    print('> info: checkout "%s" feature with default params ...'%(feature.name))
            try:
                return pipeline(**params)
            except Exception as e:
                print('> error: execute feature extraction pipeline, ', e)
        else:
            return None
        


 



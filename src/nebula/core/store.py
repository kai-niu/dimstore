import dill
import json
import copy
import uuid

from nebula.providers.persistor.persistor_factory import PersistorFactory
from nebula.providers.serializer.serializer_factory import SerializerFactory
from nebula.providers.cache.cache_layer_factory import CacheLayerFactory
from nebula.providers.meta_manager.meta_manager_factory import MetaManagerFactory 
from nebula.providers.output_render.output_render_factory import OutputRenderFactory
from nebula.providers.dataframe.processor_factory import DataframeProcessorFactory
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
        # make sure the initial uuid is always different,
        # it will make sure the uniqueness checking is accurate.
        feature.uid = feature.uid = str(uuid.uuid4())
        if self.meta_manager.is_unique(feature):
            try:
                # persist the serialized pipeline
                persistor = self.persistor_factory.get_persistor(feature.persistor)
                persistor.write(feature, dumps, **kwargs)
            except Exception as e:
                print('> the persistor write operation failed.', e)
                raise

            try:
                # add new registered feature into store catalog
                self.meta_manager.register(feature)
            except Exception as e:
                print('> the meta manager registration operation failed.', e)
                persistor.delete(feature.uid)
                raise
        else:
            print("> The feature name '%s' is not unique in namespace: %s" % (feature.name, feature.namespace))

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
        print("== %s Information ==" % (self.config['store_name']))
        print("- Meta Data Manager: %s" % (self.config['meta_manager']))
        print("- Persistor: %s" % (self.config['default_persistor']))
        print("- Serializers: %s" % (self.config['default_serializer']))
        print("- Output Render: %s" % (self.config['output_render']))
        print("- Cache Layer: %s" % (self.config['default_cache_layer']))
    


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
    " delete the features from the store
    " 
    """
    def delete(self, ufd, hard=False, verbose=True, **kwargs):
        """
        @param::ufd: the {uid:feature} dictioary
        @param::verbose: toggle the log information output
        @param::kwargs: the keyworded parameters
        return the dataframe built from feature list
        """
        # perform hard/soft deletion
        if hard:
            processed = {}
            for u, f in ufd.items():
                try:
                    persistor = self.persistor_factory.get_persistor(f.persistor)
                    persistor.delete(f.uid)
                    processed[u] = f
                except Exception as e:
                    print('> the hard deletion failed.', e)
                    self.meta_manager.delete(processed,verbose)
                    raise
            self.meta_manager.delete(processed, verbose)
        else:
            self.meta_manager.delete(ufd, verbose)

    
    """
    "
    " update the features from the store
    " 
    """
    def update(self, ufd, verbose=True, **kwargs):
        """
        @param::ufd: the {uid:feature} dictioary
        @param::verbose: toggle the log information output
        @param::kwargs: the keyworded parameters
        return the dataframe built from feature list
        """
        self.meta_manager.update(ufd, verbose)

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
        if ufd != None or len(ufd) > 0:
            if verbose:
                print('> task: build %s dataframe from %d features ...'%(dataframe, len(ufd)))
            # init
            output_df = None
            in_df = None
            index1 = None
            index2 = None  
            cold_start = True  
            for _,feature in ufd.items():
                in_df = self.checkout(feature, dataframe, verbose=verbose, **kwargs)
                if cold_start:
                    output_df = in_df
                    index1 = feature.index
                    cold_start = False
                else:
                    index2 = feature.index
                    # query supported jointers
                    jointer = DataframeProcessorFactory.get_jointer(dataframe)
                    if jointer == None:
                        raise Exception('> Store.build(): join "%s" datafram is not supported.'%(dataframe))
                    output_df = jointer.try_join(output_df,in_df,index1,index2)               
        return output_df

    """
    "
    " check out feature from persistence layer
    "
    """
    def checkout(self, feature, out_type, verbose=True, **kwargs):
        """
        @param::feature: the FeatureMetaData object
        @param::out_type: the output feature datafram type in string
        @param::verbose: the boolean value toggles log info output
        @param::kwargs: the keyworded parameter list
        return the feature extracted by executing the pipeline
        """
        # check edge case
        if out_type == None:
            raise ValueError('> Store.checkout(): the output dataframe type can not be None.')
        if not isinstance(feature, FeatureMetaData):
            raise TypeError('> Store.checkout(): the feature object is not an instance of FeatureMetaData class.')
        df_converter = DataframeProcessorFactory.get_converter(feature.output, out_type)
        if df_converter == None:
            raise Exception('> Store.checkout(): convert operation does not support the output dataframe.')
        df_normalizer = DataframeProcessorFactory.get_normalizer(feature.output)
        if df_normalizer == None:
            raise Exception('> Store.checkout(): normalizer does not support the feature dataframe.')

        # extract feature dataframe
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
                    print('> info: checkout %s feature "%s" with params: %s ...'%(feature.output, feature.name, params))
            else:
                if verbose:
                    print('> info: checkout %s feature "%s" with default params ...'%(feature.output, feature.name))
            try:
                qualified_df = df_normalizer.qualify_column(pipeline(**params),feature)
                return df_converter.astype( qualified_df, out_type)
            except Exception as e:
                print('> error: execute feature extraction pipeline, ', e)
        else:
            return None
        


 



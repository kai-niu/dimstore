"""
" 
"  functions join different dataframes together
"
"""

class DataframeBuilder():
    def __init__(self, store_proxy):
        if store_proxy == None:
            raise Exception('> DataframeBuilder __init__: store_proxy object can not be None.')
        else:
            self.__store_proxy__ = store_proxy

    """
    "
    " join pyspark dataframe df2 to df1 
    "
    """
    @staticmethod
    def pyspark_joiner(df1, df2, index1, index2):
        """
        @param::df1: pyspark dataframe
        @param::df2: pyspark dataframe
        @param::index1: df1 index column name
        @param::index2: df2 index column name
        return joined pyspark dataframe
        """
        # rename duplicated column name into full qualified feature name
        # membership check take O(1) time, should works ok when # of columns is large.
        for c in df2.columns:
            if c in df1.columns:
                df1 = df1.withColumnRenamed(c, '.'.join([]))

        df2 = df2.withColumnRenamed(index2, index1)
        df = df1.join(df2, [index1], how='inner')
        return df

    @staticmethod
    def pandas_joiner(df1,df2,index1,index2):
        pass


    """
    "
    " check out feature from persistence layer
    "
    """
    def __checkout__(self, feature, verbose=True, **kwargs):
        """
        @param::feature: the feature metadata object
        @param::verbose: boolean value toggles log info output
        @param::kwargs: the keyed parameter list
        return the feature extracted by executing the pipeline
        """
        if feature != None:
            # retrieve the serialized pipeline
            persistor = self.__store_proxy__.persistor_factory.get_persistor(feature.persistor)
            dumps = persistor.read(feature.uid, **kwargs)
            # deserialize the pipeline
            serializer = self.__store_proxy__.serializer_factory.get_serializer(feature.serializer)
            pipeline = serializer.decode(dumps, **kwargs)
            # execute the pipeline
            qualify_name = '.'.join([feature.namespace,feature.name])
            params = {}
            if qualify_name in kwargs:
                params = kwargs[qualify_name]
                if verbose:
                    print('  - params: %s ...'%(params))
            else:
                if verbose:
                    print('  - params: default values ...')
            try:
                return pipeline(**params)
            except Exception as e:
                print('> error: execute feature extraction pipeline, ', e)
        else:
            return None

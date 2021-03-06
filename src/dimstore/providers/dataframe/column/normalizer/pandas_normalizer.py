"""
"
" pandas dataframe column normalizer class
"
"""
from pandas.core.frame import DataFrame
from dimstore.core.feature_metadata import FeatureMetaBase
class PandasNormalizer():

    # class attribute
    __support__ = {'pandas'}

    """
    "
    " check normalization capability
    " 
    """
    @classmethod
    def is_capable(cls, df_type):
        """
        @param::df_type: the type of input dataframe in string
        return boolean value indicates whether or not the conversion is supported
        """
        if df_type == None:
            return False
        return df_type.lower() in cls.__support__

    """
    "
    " method that normalize columns name into fully qualified version
    "
    """
    @classmethod
    def qualify_column(cls,df,feature,**kwargs):
        """
        @param::df: the dataframe contain feature data
        @param::feature: the feature metadata object
        return the qaulified version of feature dataframe
        """
        # check edge case
        if not isinstance(df, DataFrame):
            raise TypeError('> PandasNormalizer.qualify_column(): the input dataframe is not an instance of pandas dataframe.')
        if not isinstance(feature, FeatureMetaBase):
            raise TypeError('> PysparkNormalizer.qualify_column(): the feature object is not an instance of FeatureMetaData class.')

        # qualify the column names
        cols = []
        for c in df.columns:
            if c.lower() != feature.index.lower():
                cols.append('_'.join([feature.namespace,feature.name,c]).replace('.','_'))
            else:
                cols.append(c)
        df.columns = cols
        return df

    """
    "
    " report the capability
    "
    """
    @classmethod
    def info(cls):
        """
        @param: empty intended
        return list of supported dataframe types
        """
        return list(cls.__support__)
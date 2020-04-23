"""
"
" base class of column normalizer which convert the column name into fully qualified name
"
"""

class ColumnNormalizerBase():

    def __init__(self):
        pass

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
        raise NotImplementedError('> Dataframe.JointerBase is_capable(): method is not implemented. ')

    """
    "
    " method that normalize columns name into fully qualified version
    "
    """
    @classmethod
    def qualify_columns(cls,df,feature,**kwargs):
        """
        @param::df: the dataframe contain feature data
        @param::feature: the feature metadata object
        return the qaulified version of feature dataframe
        """
        raise NotImplementedError('> NormalizerBase.qualify_column(): not implemented.')

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
        raise NotImplementedError('> Datafram.jointer info(): method is not implemented. ')
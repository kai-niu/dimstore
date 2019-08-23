"""
"
" the pyspark dataframe converter
"
"""
from pyspark.sql.dataframe import DataFrame
from nebula.providers.dataframe.converter.converter_base import ConverterBase

class PySparkConverter(ConverterBase):
    # class attributes
    __support__ = {('pyspark','pandas'),
                   ('pyspark','pyspark')}

    """
    "
    " report conversion capability
    " 
    """
    @classmethod
    def is_capable(cls, in_type, out_type):
        """
        @param::in_type: the type of input dataframe in string
        @param::out_type: the type of output dataframe in string
        return boolean value indicates whether or not the conversion is supported
        """
        return (in_type.lower(),out_type.lower()) in cls.__support__
        

    """
    "
    " perform the datafram conversion
    "
    """
    @classmethod
    def astype(cls,df,out_type,**kwargs):
        """
        @param::out_type: the type of output datafram in string
        return the converted dataframe or None if not feasible
        """
        # handle edge cases
        if not isinstance(df,DataFrame):
            raise Exception('> PySparkConverter astype(): input dataframe must be instance of pyspark dataframe class.')
        if out_type == None:
            raise ValueError('> PySparkConverter astype(): dataframe out_type parameter can not be none.')
        if not cls.is_capable('pyspark',out_type):
            raise Exception('> PySparkConverter astype(): convert to type: %s not supported.'%(out_type))

        # convert to target type
        if df != None:
            if out_type.lower() == 'pandas':
                try:
                    return df.toPandas()
                except Exception as e:
                    print('> PySparkConverter astype(): convert to pandas dataframe failed: %s'%(e))
            if out_type.lower() == 'pyspark':
                return df
        return None
            

    """
    "
    " report the capability of convert operation
    "
    """
    @classmethod
    def info(cls):
        """
        @param: empty intended
        return list of possbile join combinations
        """
        return ["[%s] => [%s]"%(i,o) for i,o in cls.__support__]
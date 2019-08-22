from nebula.providers.dataframe_jointer.dataframe_jointer_base import DataframeJointerBase
from pyspark.sql.dataframe import DataFrame

"""
" 
"  dataframe builder which builds dataframe from features as pyspark datafram object
"
"""
class PySparkDataframeJointer(DataframeJointerBase):

    # class attribute
    __support__ = {'pyspark'}

    """
    "
    " check if the jointer class can join the two dataframes of given type
    "
    """
    @classmethod
    def is_capable(cls,df_type):
        """
        @param::df_type: the type of dataframe to join
        return boolean value indicates whether or not the two dataframes can be joined.
        """
        return df_type.lower() in cls.__support__
        

    """
    "
    " try to join the two dataframes of given type and return the dataframe as specified.
    "
    """
    def try_join(self, left, right):
        """
        @param::left: the tuple contains (dataframe, index)
        @param::right: the tuple contains (dataframe, index)
        @param::out: the output dataframe type in string
        return the joined dataframe or none if the join operation is not feasible.
        """
        df1,index1 = left
        df2,index2 = right
        if not isinstance(df1, DataFrame) or not isinstance(df2, DataFrame):
            raise Exception('> error: at least one dataframe is not a instance of pyspark.sql.dataframe.DataFrame class.')
        df = self.__join__((df1,index1),(df2,index2))
        return df

    """
    "
    " report the capability of join operation
    "
    """
    @classmethod
    def possible(cls):
        """
        @param: empty intended
        return list of possbile join combinations
        """
        return list(cls.__support__)

    """
    "
    " join pyspark dataframe df2 to df1 
    "
    """
    def __join__(self, left_grp, right_grp):
        """
        @param::left_grp: the tuple contains (pyspark dataframe, index)
        @param::right_grp: the tuple contains (pyspark dataframe, index)
        return joined pyspark dataframe
        """
        df1,index1 = left_grp
        df2,index2 = right_grp
        df2 = df2.withColumnRenamed(index2, index1)
        df = df1.join(df2, [index1], how='inner')
        return df



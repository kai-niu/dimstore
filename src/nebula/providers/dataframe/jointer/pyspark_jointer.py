from nebula.providers.dataframe.jointer.jointer_base import JointerBase
from pyspark.sql.dataframe import DataFrame

"""
" 
"  dataframe builder which builds dataframe from features as pyspark datafram object
"
"""
class PySparkJointer(JointerBase):

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
    " try to join the two pyspark dataframes.
    "
    """
    @classmethod
    def try_join(cls, left_df, right_df, left_idx, right_idx, **kwargs):
        """
        @param::left_df: the left pyspark dataframe of join operation
        @param::right_df: the right pyspark dataframe of join operation
        @param::left_idx: the index of left pyspark dataframe of join operation
        @param::right_idx: the index of right pyspark dataframe of join operation
        return the joined dataframe or none if the join operation is not feasible.
        """
        # check edge case
        if not isinstance(left_df, DataFrame):
            raise TypeError('> PySparkJointer try_join(): left dataframe is not a instance of pyspark.sql.dataframe.DataFrame class.')
        if not isinstance(right_df, DataFrame):
            raise TypeError('> PySparkJointer try_join(): right datarame  is not a instance of pyspark.sql.dataframe.DataFrame class.')
        if left_idx not in left_df.columns:
            raise IndexError('> PySparkJointer try_join(): the index is invalid for the left dataframe instance.')
        if right_idx not in right_df.columns:
            raise IndexError('> PySparkJointer try_join(): the index is invalid for the right dataframe instance.')

        # perform join
        df = cls.__join__((left_df,left_idx),(right_df,right_idx))
        return df

    """
    "
    " report the capability of join operation
    "
    """
    @classmethod
    def info(cls):
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
    @classmethod
    def __join__(cls, left_grp, right_grp):
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



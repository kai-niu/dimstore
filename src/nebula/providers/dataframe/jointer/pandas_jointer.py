from nebula.providers.dataframe.jointer.jointer_base import JointerBase
from pandas.core.frame import DataFrame

"""
" 
"  dataframe builder which builds dataframe from features as pandas datafram object
"
"""
class PandasJointer(JointerBase):

    # class attribute
    __support__ = {'pandas'}

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
        @param::left_df: the left pandas dataframe of join operation
        @param::right_df: the right pandas dataframe of join operation
        @param::left_idx: the index of left pandas dataframe of join operation
        @param::right_idx: the index of right pandas dataframe of join operation
        return the joined dataframe or none if the join operation is not feasible.
        """
        # check edge case
        if not isinstance(left_df, DataFrame):
            raise TypeError('> PandasJointer try_join(): left dataframe is not a instance of pandas.core.frame.DataFrame class.')
        if not isinstance(right_df, DataFrame):
            raise TypeError('> PandasJointer try_join(): right datarame  is not a instance of pandas.core.frame.DataFrame class.')
        if left_idx not in left_df.columns and left_idx not in left_df.index.names:
            raise IndexError('> PandasJointer try_join(): the index is invalid for the left dataframe instance.')
        if right_idx not in right_df.columns and right_idx not in right_df.index.names:
            raise IndexError('> PandasJointer try_join(): the index is invalid for the right dataframe instance.')

        # make sure index assigned 
        if left_idx not in left_df.index.names:
            left_df = left_df.set_index(left_idx)
        if right_idx not in right_df.index.names:
            right_df = right_df.set_index(right_idx)

        # perform join
        df = cls.__join__(left_df,right_df)
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
    def __join__(cls, left_df, right_df):
        """
        @param::left_df: the pandas dataframe as left param of join operation
        @param::right_df: the pandas dataframe as right param of join operation
        return joined pyspark dataframe
        """
        df = left_df.join(right_df, how='inner')
        return df



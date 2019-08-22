"""
"
" the base class of dataframe joiner
"
"""

class DataframeJointerBase():

    def __init__(self):
        pass

    """
    "
    " check if the jointer class can join the two dataframes of given type
    "
    """
    @classmethod
    def is_capable(self, left, right, out):
        """
        @param::left: the type of left dataframe in string
        @param::right: the type of right dataframe in string
        @param::out: the output dataframe type in string
        return boolean value indicates whether or not the two dataframes can be joined.
        """
        raise NotImplementedError('> DataframeJointerBase is_capable: method is not implemented. ')

    """
    "
    " try to join the two dataframes of given type and return the dataframe as specified.
    "
    """
    def try_join(self, left, right, out):
        """
        @param::left: the tuple contains (dataframe, type, index)
        @param::right: the tuple contains (dataframe, type, index)
        @param::out: the output dataframe type in string
        return the joined dataframe or none if the join operation is not feasible.
        """
        raise NotImplementedError('> DataframeJointerBase try_join: method is not implemented. ')

    """
    "
    " report the capability of join operation
    "
    """
    @classmethod
    def possible(self):
        """
        @param: empty intended
        return list of possbile join combinations
        """
        raise NotImplementedError('> DataframeJointerBase try_join: method is not implemented. ')
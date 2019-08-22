from nebula.providers.dataframe_jointer.pyspark_dataframe_jointer import PySparkDataframeJointer

class DataframeJointerFactory():

    # class varaible
    __jointers__ = [PySparkDataframeJointer]    

    """
    "
    " get the first jointer that support the join operation
    "
    """
    @classmethod
    def get(cls, left, right, out):
        """
        @param::left: the type of left dataframe in string
        @param::right: the type of right dataframe in string
        @param::out: the output dataframe type in string
        return boolean value indicates whether or not the two dataframes can be joined.
        """
        for j in cls.__jointers__:
            if j.is_capable(left,right,out):
                return j()

    """
    "
    " report the info of datafram jointer factory
    "
    """
    @classmethod
    def info(cls):
        possible_joints = []
        for j in cls.__jointers__:
            possible_joints.extend(j.possible())
        return possible_joints 
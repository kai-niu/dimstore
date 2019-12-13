"""
"
" the dataframe converter base class
"
"""

class ConverterBase():
    def __init__(self):
        pass

    """
    "
    " check conversion capability
    " 
    """
    @classmethod
    def is_capable(cls, in_type, out_type):
        """
        @param::in_type: the type of input dataframe in string
        @param::out_type: the type of output dataframe in string
        return boolean value indicates whether or not the conversion is supported
        """
        raise NotImplementedError('> Dataframe.JointerBase is_capable(): method is not implemented. ')

    """
    "
    " perform the datafram conversion
    "
    """
    @classmethod
    def astype(cls,out_type,**kwargs):
        """
        @param::out_type: the type of output datafram in string
        return the converted dataframe or None if not feasible
        """
        raise NotImplementedError('> Dataframe.JointerBase astype(): method is not implemented. ')

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
        raise NotImplementedError('> Datafram.jointer info(): method is not implemented. ')
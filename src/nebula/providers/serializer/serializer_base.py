"""
    base class of serializer
"""

class SerializerBase():
    def __init__(self):
        pass
    
    def encode(self, obj, **kwargs):
        raise NotImplementedError('Serializer encode method implementation error!')

    def decode(self, dumps, **kwargs):
        raise NotImplementedError('Serializer decode method implementation error!')
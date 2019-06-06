"""
    base class of serializer
"""

class SerializerBase():
    def __init__(self):
        pass
    
    def encode(self, obj, **kwargs):
        raise Exception('Serializer encode method implementation error!')

    def decode(self, dumps, **kwargs):
        raise Exception('Serializer decode method implementation error!')
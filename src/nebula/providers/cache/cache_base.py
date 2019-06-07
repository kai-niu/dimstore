"""
    base cache layer class
"""

class CacheLayerBase():
    
    def __init__(self):
        pass

    def put(self, key, value, **kwargs):
        raise NotImplementedError('Cache put method implementation error.')

    def get(self, key, **kwargs):
        raise NotImplementedError('Cache get method implementation error.')

    def stats(self, **kwargs):
        raise NotImplementedError('Cache stats method implementation error.')
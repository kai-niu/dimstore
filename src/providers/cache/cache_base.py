"""
    base cache layer class
"""

class CacheLayerBase():
    
    def __init__(self):
        pass

    def put(self, key, value, **kwargs):
        raise Exception('Cache put method implementation error.')

    def get(self, key, **kwargs):
        raise Exception('Cache get method implementation error.')

    def stats(self, **kwargs):
        raise Exception('Cache stats method implementation error.')
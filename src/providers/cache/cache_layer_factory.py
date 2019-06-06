"""
    cache layer factory
"""

class CacheLayerFactory():

    def __init__(self, config):
        self.config = config

    # meta manager factory
    def get_cache_layer(self):
        pass

    # return supported meta manager info
    def info(self):
        return ['None']
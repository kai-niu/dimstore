"""
    default way to serialize the feature extraction pipeline
"""

import dill
from nebula.providers.serializer.serializer_base import SerializerBase

class DillSerializer(SerializerBase):
    
    def __init__(self, config):
        self.config = config

    def encode(self, obj, **kwargs):
        dill.settings['recurse'] = True
        dumps = dill.dumps(obj)
        return dumps

    def decode(self, dumps, **kwargs):
        pipeline = dill.loads(dumps)
        return pipeline


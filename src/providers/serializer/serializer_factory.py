"""
    serializer factory
"""
from src.providers.serializer.dill_serializer import DillSerializer

class SerializerFactory():

    def __init__(self, config):
        self.config = config

    # book keeper factory
    def get_serializer(self, type):
        if type == 'default':
            return DillSerializer(self.config['serializer_providers'])

    # return supported serializer info
    def info(self):
        return ['dill Serializer (default)']
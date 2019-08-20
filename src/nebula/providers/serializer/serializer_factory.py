"""
    serializer factory
"""
from nebula.providers.serializer.dill_serializer import DillSerializer

class SerializerFactory():

    def __init__(self, config):
        self.config = config

    # book keeper factory
    def get_serializer(self, type):
        if type == 'dill_serializer':
            return DillSerializer(self.config['serializer_providers']['dill_serializer'])

    # return supported serializer info
    def info(self):
        return ['dill_serializer: dill Serializer.']
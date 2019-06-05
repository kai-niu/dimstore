"""
    serializer factory
"""
from src.providers.dill_serializer import Dill_Serializer

class Serializer_Factory():

    def __init__(self, config):
        self.config = config

    # book keeper factory
    def get_serializer(self, type):
        if type == 'default':
            return Dill_Serializer(self.config['serializer_providers'])

    # return supported serializer info
    def info(self):
        return ['dill Serializer (default)']
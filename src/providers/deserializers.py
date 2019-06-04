import dill

"""
    default way to deserialize the feature extraction pipeline
"""
def default_deserializer(dumps, config, **kwargs):
    pipeline = dill.loads(dumps)
    return pipeline


"""
" Register all deserializer
"""
class Deserializer_Registration():

    def __init__(self):
        self.providers = {'default': default_deserializer \
                         }
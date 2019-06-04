import dill

"""
    default way to serialize the feature extraction pipeline
"""
def default_serializer(pipeline, config, **kwargs):
    dill.settings['recurse'] = True
    content = dill.dumps(pipeline)
    return content

"""
" Register all serializer
"""
class Serializer_Registration():

    def __init__(self):
        self.providers = {'default': default_serializer \
                         }
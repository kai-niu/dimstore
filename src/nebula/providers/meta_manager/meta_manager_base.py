"""
    base class of meta manager class
"""


class MetaManagerBase():

    def __init__(self, config):
        pass

    def register(self, feature):
        raise NotImplementedError('Meta Manager register method implementation error!')

    def lookup(self, name, namespace='default', **kwargs):
        raise NotImplementedError('Meta Manager lookup method implementation error!')

    def list_features(self, namespace='default', match_child=True, **kwargs):
        raise NotImplementedError('Meta Manager list_features method implementation error!')

    def inspect_feature(self, uid, **kwargs):
        raise NotImplementedError('Meta Manager inspect_feature method implementation error!')

    def remove_feature(self, uid, **kwargs):
        raise NotImplementedError('Meta Manager remove_feature method implementation error!')

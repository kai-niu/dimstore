"""
    base class of meta manager class
"""


class Meta_Manager_Base():

    def __init__(self, config):
        pass

    def register(self, feature):
        raise Exception('Meta Manager register method implementation error!')

    def lookup(self, uid):
        raise Exception('Meta Manager lookup method implementation error!')

    def list_features(self, **kwargs):
        raise Exception('Meta Manager list_features method implementation error!')

    def inspect_feature(self, uid, **kwargs):
        raise Exception('Meta Manager inspect_feature method implementation error!')

    def remove_feature(self, uid, **kwargs):
        raise Exception('Meta Manager remove_feature method implementation error!')



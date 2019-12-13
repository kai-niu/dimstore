"""
    output render base class
"""


class OutputRenderBase():

    def __init__(self, config):
        pass

    def store_detail(self, output):
        raise NotImplementedError('store detail render method not implemented!')

    def feature_list(self, output):
        raise NotImplementedError('feature list render method not implemented!')

    def namespace_list(self, output):
        raise NotImplementedError('namespace list render method not implemented!')

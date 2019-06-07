"""
    persistor base class
"""

class PersistorBase():
    def __init__(self):
        pass
    
    def write(self, feature, dumps, **kwargs):
        raise NotImplementedError("Persistor write method implementation error!")

    def read(self, uid, **kwargs):
        raise NotImplementedError("Persistor read method implementation error!")

    def delete(self, uid, **kwargs):
        raise NotImplementedError("Persistor delete method implementation error!")
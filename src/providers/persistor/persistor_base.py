"""
    persistor base class
"""

class PersistorBase():
    def __init__(self):
        pass
    
    def write(self, feature, dumps, **kwargs):
        raise Exception("Persistor write method implementation error!")

    def read(self, uid, **kwargs):
        raise Exception("Persistor read method implementation error!")

    def delete(self, uid, **kwargs):
        raise Exception("Persistor delete method implementation error!")
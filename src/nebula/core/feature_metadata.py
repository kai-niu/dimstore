import uuid
from datetime import datetime

"""
"  use dictionary instead of class attributes enable deserialization backward compatibility,
"  add new meta data member will still be able to deserialize from old object dumps.
"  !important idea is to keep this poco object as simple as possible to enable the backward 
"  compatbility of serialization/deserialization process.
"""
class FeatureMetaBase(object):
    
    def __init__(self):
        self.metadata = {}

class FeatureMetaData(FeatureMetaBase):

    def __init__(self, name, index, output, namespace = 'default'):
        super().__setattr__('metadata', 
                                                {
                                                    'uid': str(uuid.uuid4()),
                                                    'name': name,
                                                    'index': index,
                                                    'namespace': namespace,
                                                    'author':"",
                                                    'tags': set([]),
                                                    'params':{},
                                                    'output':output,
                                                    'comment':'',
                                                    'persistor':None,
                                                    'serializer':None,
                                                    'create_date': datetime.today()
                                                })

    def __getattr__(self,key):
        if key in self.metadata:
            return self.metadata[key]
        else:
            return None

    def __setattr__(self,key,value):
        self.metadata[key] = value

    def __getstate__(self): 
        return self.__dict__

    def __setstate__(self, d): 
        self.__dict__.update(d)
      


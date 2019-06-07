import uuid
from datetime import datetime

class FeatureMeta():

    def __init__(self, name, params_list=None, \
                             params_description = None, \
                             comment=None, \
                             persistor = 'default', 
                             serializer = 'default'):
        self.name = name
        self.uid = uuid.uuid4()
        self.author = ""
        self.create_date = datetime.today()
        self.params = {}
        self.comment = comment
        self.persistor = persistor
        self.serializer = serializer

import uuid

class Feature():

    def __init__(self, name, params_list=None, \
                             params_description = None, \
                             comment=None, \
                             reader = 'default', \
                             writer = 'default', \
                             serializer = 'default', \
                             deserializer = 'default'):
        self.name = name
        self.uid = uuid.uuid4()
        self.params_list = None
        self.params_description = None
        self.comment = comment
        self.reader = reader
        self.writer = writer
        self.serializer = serializer
        self.deserializer = deserializer

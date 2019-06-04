from src.utility.file_functions import write_binary_file

"""
"  default way to persist the serialized content
"""
def default_writer(feature, dumps, config, **kwargs):
    path = '%s/%s' % (config['root_dir'], config['writers']['default']['folder_name'])
    filename = '%s.dill'%(feature.uid)
    write_binary_file(path,filename,dumps)


"""
" Register all writers
"""
class Writer_Registration():

    def __init__(self):
        self.providers = {'default': default_writer \
                         }
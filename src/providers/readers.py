from src.utility.file_functions import file_exist, read_binary_file

"""
" default way to read the serialized content
"""
def default_reader(id, config, **kwargs):
    path = '%s/%s' % (config['root_dir'], config['readers']['default']['folder_name'])
    filename = '%s.dill'%(id)

    if file_exist(path, filename):
        content = read_binary_file(path, filename)
        return content

    return None


"""
" Register all readers
"""
class Reader_Registration():

    def __init__(self):
        self.providers = {'default': default_reader \
                         }
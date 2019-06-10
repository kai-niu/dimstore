"""
    configuration builder
"""
from nebula.utility.config_template import ConfigTemplate
from nebula.utility.file_functions import write_text_file
import json

class ConfigBuilder():

    def __init__(self, type='default'):

        # init config template
        template = ConfigTemplate()

        if type in template.catalog:
            self.config = template.catalog[type]
        else:
            self.config = template.catalog['default']


    def build(self, path, filename='store_config'):
        dump = json.dumps(self.config)
        fname = '%s.json'%(filename)
        write_text_file(path,fname,dump)

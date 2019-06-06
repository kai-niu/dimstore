"""
    config template
"""

class ConfigTemplate():

    def __init__(self):

        # tempalte collections
        default = {
            "store_name": "IBM Feature Store",
            "meta_manager": "default",
            "meta_manager_providers": {
                                        "default":{"root_dir":"path_to_root_storage_folder",
                                                   "folder_name":"catalog",
                                                   "file_name":"catalog.nbl"}
                                      },
            "persistor_providers": {
                                        "default":{"root_dir":"path_to_root_storage_folder",
                                                 "folder_name":"features"}
                                    },
            "serializer_providers": {"default":{}},
            "cache_providers": {}

        }

        # template catalog
        self.catalog = {'default': default}

        
                        
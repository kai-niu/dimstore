"""
    test persistor factory class
"""
import pytest
from unittest import mock
from nebula.providers.persistor.persistor_factory import PersistorFactory
from nebula.providers.persistor.persistor_base import PersistorBase

@pytest.fixture(scope='function')
def mock_config(request):
    # mock the config object
    config = {
                "store_name": "Kai's Feature Store",
                "default_meta_manager": "flat_file_meta_manager",
                "default_persistor": "ibm_object_storage",
                "default_serializer": "dill_serializer",
                "meta_manager_providers": {
                    "flat_file_meta_manager":{
                        "root_dir": "/foobar",
                        "folder_name":"catalog",
                        "file_name":"catalog.nbl"
                    }
                },
                "persistor_providers":{
                    "flat_file_storage":{
                        "root_dir": "/foobar",
                        "folder_name":"features"
                    },
                    "ibm_object_storage":{
                        "iam_service_id": "foobar",
                        "ibm_api_key_id": "foobar",
                        "endpoint":"https://s3.us.cloud-object-storage.appdomain.cloud",
                        "ibm_auth_endpoint": "https://iam.bluemix.net/oidc/token",
                        "bucket": "foobar-bucket"
                    }
                },
                "serializer_providers":{
                    "default":{
                    }
                }
            }
    return config

class TestIBMObjectStoragePersistorFactory():
    def test_init_parse_config(self, mock_config):
        # arrange
        pass
        # action
        factory = PersistorFactory(mock_config)
        # assert
        assert factory.config['store_name'] == "Kai's Feature Store"

    def test_get_persistor_result(self, mock_config):
        # arrange
        factory = PersistorFactory(mock_config)
        type_index = ['flat_file_storage','ibm_object_storage']
        # action/assert
        for t in type_index:
            persistor = factory.get_persistor(t)
            assert isinstance(persistor, PersistorBase)

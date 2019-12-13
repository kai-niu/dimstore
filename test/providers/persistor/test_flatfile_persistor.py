"""
    test flatfile persistor
"""
import pytest
from unittest import mock
from nebula.providers.persistor.flatfile_persistor import FlatFilePersistor
from nebula.core.feature_metadata import FeatureMetaData


@pytest.fixture(scope='function')
def mock_class(request):
    # mock the persistor with fixed connection to ibm object storage
    config = '{}'
    client = FlatFilePersistor(config)
    return client

@pytest.fixture(scope='function')
def mock_feature_meta(request):
    # mock the feature meta data class
    meta = FeatureMetaData('test','id','pyspark')
    meta.author ='test author'
    return meta

@pytest.fixture(scope='function')
def mock_config(request):
    # mock the config object
    config = {
            "root_dir": "/foobar",
            "folder_name":"catalog",
            "file_name":"catalog.nbl"
            }
            
    return config



class TestIBMObjectStoragePersistorBase():
    def test_init_parse_config(self, mock_config):
        with mock.patch('nebula.providers.persistor.ibm_object_storage_persistor.ibm_boto3') as mocked_boto:
            # arrange
            config = mock_config
            mock.patch.object(mocked_boto,'client', return_value=None)  
            # act
            obj = FlatFilePersistor(config)
           
            # assert
            assert obj.path == '/foobar/catalog'

    def test_write_method_call_put_write_binary_method_once(self, mock_config, mock_feature_meta):
        with mock.patch('nebula.utility.file_functions.write_binary_file') as mocked_write_fn:
            # arrange
            config = mock_config
            mock_persistor = FlatFilePersistor(config)
            mocked_write_fn.return_value = None
            # act
            mock_persistor.write(mock_feature_meta, None) 
            # assert
            mocked_write_fn.write_binary_file.assert_called_once()
    
   
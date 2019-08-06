"""
    test ibm object storage persistor
"""
import pytest
from unittest import mock
from nebula.providers.persistor.ibm_object_storage_persistor import IBMObjectStoragePersistor
from nebula.core.feature_meta import FeatureMeta
import ibm_boto3

@pytest.fixture(scope='function')
def mock_class(request):
    # mock the persistor with fixed connection to ibm object storage
    config = '{}'
    client = IBMObjectStoragePersistor(config)
    return client

@pytest.fixture(scope='function')
def mock_feature_meta(request):
    # mock the feature meta data class
    meta = FeatureMeta('test')
    meta.author ='test author'
    return meta



class TestIBMObjectStoragePersistorBase():

    def test_init_call_boto_client_once(self, mock_feature_meta):
        with mock.patch('nebula.providers.persistor.ibm_object_storage_persistor.ibm_boto3') as mocked_boto:
            config = '{}'
            IBMObjectStoragePersistor(config)  
            mocked_boto.client.assert_called_once()

    def test_write_method_call_put_object_once(self, mock_feature_meta):
        with mock.patch('nebula.providers.persistor.ibm_object_storage_persistor.ibm_boto3') as mocked_boto:
            # create persistor instance
            config = '{}'
            mock_persistor = IBMObjectStoragePersistor(config)
            # mock boto client object
            mock.patch.object(mocked_boto.client,'put_object',return_value=None)
            # test
            mock_persistor.write(mock_feature_meta, None)    
            mock_persistor.client.put_object.assert_called_once()
    
    def test_write_method_call_put_object_exception(self, mock_feature_meta):
        with mock.patch('nebula.providers.persistor.ibm_object_storage_persistor.ibm_boto3'):
            # create persistor instance
            config = '{}'
            mock_persistor = IBMObjectStoragePersistor(config)
            # mock boto client object
            mock_persistor.client.put_object.side_effect = Exception('Test Exception')
    
            # test
            with pytest.raises(Exception): 
                mock_persistor.client.put_object()

            mock_persistor.write(mock_feature_meta, None) # supposed to catch exception
    



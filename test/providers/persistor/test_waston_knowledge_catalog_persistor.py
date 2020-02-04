"""
    test ibm wkc persistor
"""
import pytest
from unittest import mock
from nebula.providers.persistor.waston_knowledge_catalog_persistor import WastonKnowlegeCatalogPersistor
from nebula.core.feature_metadata import FeatureMetaData


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
                "catalog_name": "DimStore",
                "uid": "foo",
                "token": "bar",
                "host": "www.clusterfoo.com"
             }      
    return config

class TestWastonKnowledgeCatalogPersistor():

    def test_init_parse_config(self, mock_config):
        with mock.patch('nebula.providers.persistor.waston_knowledge_catalog_persistor.WastonKnowledgeCatalogClient') as wkc_client:
            # arrange
            config = mock_config
            wkc_client.return_value = mock.MagicMock(return_value='foo client')
            # act
            obj = WastonKnowlegeCatalogPersistor(config)
            # assert
            assert obj.catalog_name == 'DimStore'
            assert obj.uid == 'foo'
            assert obj.pwd =='bar'
            assert obj.host == 'www.clusterfoo.com'
            assert obj.client() == 'foo client'

    def test_is_feature_asset_exist_in_wkc_true(self, mock_config):
        with mock.patch('nebula.providers.persistor.waston_knowledge_catalog_persistor.WastonKnowledgeCatalogClient') as wkc_client:
            # arrange
            config = mock_config
            wkc_client.return_value.get_asset_types.return_value=['feature_asset']
            # act
            mock_persistor = WastonKnowlegeCatalogPersistor(config)
            # act/assert
            assert mock_persistor.client.get_asset_types() == ['feature_asset']
            assert mock_persistor.__is_feature_asset_exist__() == True
    
    def test_is_feature_asset_exist_in_wkc_false(self, mock_config):
        with mock.patch('nebula.providers.persistor.waston_knowledge_catalog_persistor.WastonKnowledgeCatalogClient') as wkc_client:
            # arrange
            config = mock_config
            wkc_client.return_value.get_asset_types.return_value=['foo_asset']
            # act
            mock_persistor = WastonKnowlegeCatalogPersistor(config)
            # act/assert
            assert mock_persistor.client.get_asset_types() == ['foo_asset']
            assert mock_persistor.__is_feature_asset_exist__() == False

    def test_call_wkc_client_create_asset_type_method_once(self, mock_config):
        with mock.patch('nebula.providers.persistor.waston_knowledge_catalog_persistor.WastonKnowledgeCatalogClient') as wkc_client:
            # arrange
            config = mock_config
            wkc_client.return_value.get_asset_types.return_value=['foo_asset']
            # act
            WastonKnowlegeCatalogPersistor(config)
            # assert 
            wkc_client.return_value.create_asset_type.assert_called_once()
            wkc_client.return_value.get_asset_types.assert_called_once()

    def test_call_wkc_client_create_asset_type_method_zero(self, mock_config):
        with mock.patch('nebula.providers.persistor.waston_knowledge_catalog_persistor.WastonKnowledgeCatalogClient') as wkc_client:
            # arrange
            config = mock_config
            wkc_client.return_value.get_asset_types.return_value=['feature_asset']
            # act
            WastonKnowlegeCatalogPersistor(config)
            # assert 
            wkc_client.return_value.create_asset_type.assert_not_called()
            wkc_client.return_value.get_asset_types.assert_called_once()

    



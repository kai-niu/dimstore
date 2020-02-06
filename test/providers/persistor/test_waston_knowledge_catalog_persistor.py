"""
    test ibm wkc persistor
"""
import pytest
import json
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

    def test_init_wkc_client_exception(self, mock_config):
        with mock.patch('nebula.providers.persistor.waston_knowledge_catalog_persistor.WastonKnowledgeCatalogClient') as wkc_client:
            # arrange
            config = mock_config
            mock_exception = Exception('test exception!')
            wkc_client.side_effect = mock_exception
            # assert
            with pytest.raises(Exception):
                assert WastonKnowlegeCatalogPersistor(config)

    def test_write_method_update_feature_uid(self, mock_config, mock_feature_meta):
        with mock.patch('nebula.providers.persistor.waston_knowledge_catalog_persistor.WastonKnowledgeCatalogClient') as wkc_client:
            # arrange
            config = mock_config
            feature = mock_feature_meta
            uid = feature.uid
            dumps = b' '
            response = {'metadata':{'asset_id':'foo_uid_123'}}
            wkc_client.return_value.create_asset.return_value = response
            # act
            persistor = WastonKnowlegeCatalogPersistor(config)
            persistor.write(feature,dumps)
            # assert
            assert feature.uid != uid
            assert feature.uid == 'foo_uid_123'
            wkc_client.return_value.create_asset.assert_called_once()

    def test_write_method_exception(self, mock_config, mock_feature_meta):
        with mock.patch('nebula.providers.persistor.waston_knowledge_catalog_persistor.WastonKnowledgeCatalogClient') as wkc_client:
            # arrange
            config = mock_config
            feature = mock_feature_meta
            uid = feature.uid
            dumps = b' '
            wkc_client.return_value.create_asset.side_effect = Exception('test exception!')
            # assert
            with pytest.raises(Exception):
                persistor = WastonKnowlegeCatalogPersistor(config)
                persistor.write(feature,dumps)
            assert feature.uid == uid
            wkc_client.return_value.create_asset.assert_called_once()

    def test_read_method_return_content(self, mock_config):
        with mock.patch('nebula.providers.persistor.waston_knowledge_catalog_persistor.WastonKnowledgeCatalogClient') as wkc_client:
            with mock.patch('nebula.providers.persistor.waston_knowledge_catalog_persistor.base64') as mock_base64:
                # arrange
                config = mock_config
                uid = 'foo_id'
                mock_base64.decodebytes.return_value = 'foo content'
                wkc_client.return_value.checkout_asset.return_value = 'dumps'
                # act
                persistor = WastonKnowlegeCatalogPersistor(config)
                content = persistor.read(uid)
                # assert
                assert content == 'foo content'
                wkc_client.return_value.checkout_asset.assert_called_once()
                mock_base64.decodebytes.assert_called_once()

    def test_read_method_exception(self, mock_config):
        with mock.patch('nebula.providers.persistor.waston_knowledge_catalog_persistor.WastonKnowledgeCatalogClient') as wkc_client:
            with mock.patch('nebula.providers.persistor.waston_knowledge_catalog_persistor.base64') as mock_base64:
                # arrange
                config = mock_config
                uid = 'foo_id'
                mock_base64.decodebytes.return_value = 'foo encoded'
                wkc_client.return_value.checkout_asset.side_effect = Exception('test exception!')
                # act
                persistor = WastonKnowlegeCatalogPersistor(config)
                # assert
                with pytest.raises(Exception):
                    persistor.read(uid)
                wkc_client.return_value.checkout_asset.assert_called_once()
                mock_base64.decodebytes.assert_not_called()

    def test_delete_method_success(self, mock_config):
        with mock.patch('nebula.providers.persistor.waston_knowledge_catalog_persistor.WastonKnowledgeCatalogClient') as wkc_client:
            # arrange
            config = mock_config
            uid = 'foo_id'
            wkc_client.return_value.delete_asset.return_value = None
            # act
            persistor = WastonKnowlegeCatalogPersistor(config)
            persistor.delete(uid)
            # assert
            wkc_client.return_value.delete_asset.assert_called_once()

    def test_delete_method_exception(self, mock_config):
        with mock.patch('nebula.providers.persistor.waston_knowledge_catalog_persistor.WastonKnowledgeCatalogClient') as wkc_client:
            # arrange
            config = mock_config
            uid = 'foo_id'
            wkc_client.return_value.delete_asset.side_effect = Exception('test exception!')
            # act
            persistor = WastonKnowlegeCatalogPersistor(config)
            # assert
            with pytest.raises(Exception):
                persistor.delete(uid)
            wkc_client.return_value.delete_asset.assert_called_once()

    def test_decode_endcode_method(self, mock_config):
        with mock.patch('nebula.providers.persistor.waston_knowledge_catalog_persistor.WastonKnowledgeCatalogClient'):
            # arrange
            config = mock_config
            persistor = WastonKnowlegeCatalogPersistor(config)
            # act
            code_in = b'ilovethisgame'
            encoded_out = persistor.__encode__(code_in)
            decoded_out = persistor.__decode__(encoded_out)
            # assert
            assert code_in == decoded_out
            assert code_in != encoded_out
    





    



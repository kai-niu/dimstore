"""
"   Use the IBM Waston Knowledge Catalog as persist layer
"   docu: https://cloud.ibm.com/apidocs/watson-data-api#introduction
"   docu: https://developer.ibm.com/api/view/watsondata-prod:watson-data:title-Watson_Data_API#Introduction
"""
from nebula.providers.persistor.persistor_base import PersistorBase
from nebula.utility.waston_knowledge_catalog_client import WastonKnowledgeCatalogClient
import json
import base64
import warnings

warnings.filterwarnings("ignore")

class WastonKnowlegeCatalogPersistor(PersistorBase):
    def __init__(self, config):
        self.config = config
        self.catalog_name = config['catalog_name']
        self.uid = config['uid']
        self.pwd = config['token']
        self.host = config['host']
        self.client = self.__get_wkc_client__()

        # create feature asset type in WKC
        self.__create_feature_asset__()


    """
    "
    " create the new asset in WKC from feature dumps and metadata
    "
    """
    def write(self, feature, dumps, **kwargs):
        """
        @param::feature: instance of Feature class
        @param::dumps: the byte codes of pipeline dumps
        @param::kwargs: name parameter list
        return none
        """
        # build the wkc meta data document
        metadata = {    
            "metadata": {
                    "name": feature.name,
                    "description": feature.comment,
                    "tags": list(feature.tags),
                    "asset_type": "feature_asset",
                    "origin_country": "us",
                    "rov": {
                        "mode": 0
                    }
            },
            "entity": {
                "feature_asset":{
                    "namespace": feature.namespace,
                    "dumps": self.__encode__(dumps)
                }
            }
        }
        try:
            response = self.client.create_asset(json.dumps(metadata))
            feature.uid = response['metadata']['asset_id']
        except Exception as e:
            print('> ibm wkc client create asset failed!', e)
            raise

     
  
    """
    "
    " read the feature asset from KWC and extract the byte dumps
    "
    """
    def read(self, uid, **kwargs):
        """
        @param::uid: symbolic string name used to identify the feature
        @param::kwargs: named parameter list
        return the byte dumps of feature asset
        """
        content = None
        try:
            dumps = self.client.checkout_asset(uid)
            content = self.__decode__(dumps)
        except Exception as e:
            print('> ibm WKC client read feature asset failed! \n',e)
            raise
        return content

    def delete(self, uid, **kwargs):
        """
        @param::uid: symbolic string name used to identify the feature
        @param::kwargs: named parameter list
        return none
        """
        try:
            self.client.delete_asset(uid)
        except Exception as e:
            print('> IBM WKC client delete asset failed! \n',e)
            raise


    def __get_wkc_client__(self):
        """
        @param::none:
        return the ibm wkc client instance
        """
        client = None
        try:
            client = WastonKnowledgeCatalogClient(self.catalog_name, self.host, uid=self.uid, pwd=self.pwd, verbose=False)
        except Exception as e:
            print('> ibm wkc client initialization failed! \n', e)
            raise

        return client


    def __encode__(self, dumps):
        """
        @param bytes::dumps: the feature pipeline bytes dumps
        return ascii encoded string using base64
        """
        return base64.encodebytes(dumps).decode('ascii')

    def __decode__(self, dumps):
        """
        @param string::dumps: the feature pipeline encoded as string
        return the orignal byte encoding produced from serialization layer
        """
        return base64.decodebytes(dumps.encode())

    """
    "
    " create the feature_asset type in WKC if the it is not exist.
    "
    """
    def __create_feature_asset__(self):
        """
        @param::none:
        @return none
        """
        # check if the feature_asset type exist
        if not self.__is_feature_asset_exist__():
            # define feature type
            asset_type = {
                            "description": "DimStore feature type",
                            "fields": [
                            {
                                "key": "namespace",
                                "type": "string",
                                "facet": False,
                                "is_array": False,
                                "search_path": "namespace",
                                "is_searchable_across_types": False
                            }
                        ]
                        }
            self.client.create_asset_type('feature_asset', json.dumps(asset_type))

    """
    "
    " check if the feature_asset type exist
    "
    """
    def __is_feature_asset_exist__(self):
        """
        @param::none:
        return boolean value indicates whether or not the feature_asset is created.
        """
        return 'feature_asset' in self.client.get_asset_types()
            



#         ┌─┐       ┌─┐
#      ┌──┘ ┴───────┘ ┴──┐
#      │                 │
#      │       ───       │
#      │  ─┬┘       └┬─  │
#      │                 │
#      │       ─┴─       │
#      │                 │
#      └───┐         ┌───┘
#          │         │
#          │         │
#          │         │
#          │         └──────────────┐
#          │                        │
#          │                        ├─┐
#          │                        ┌─┘
#          │                        │
#          └─┐  ┐  ┌───────┬──┐  ┌──┘
#            │ ─┤ ─┤       │ ─┤ ─┤
#            └──┴──┘       └──┴──┘
#                 BLESSING FROM 
#           THE BUG-FREE MIGHTY BEAST


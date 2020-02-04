
import http.client
import ssl
import json
from io import StringIO


"""
"
" The client interact with Waston Kownledge Catalog using Waston data API(beta)
" docu: https://cloud.ibm.com/apidocs/watson-data-api#introduction
" docu: https://developer.ibm.com/api/view/watsondata-prod:watson-data:title-Watson_Data_API#Introduction
" docu: https://tools.ietf.org/html/rfc6902
"
"""

class WastonKnowledgeCatalogClient():
    
    def __init__(self, catalog_name, host, uid=None, pwd=None, token=None, verbose=True):
        """
        @param::token: authentication token in string
        @param::host: host url in string
        return catalog client instance
        """
        if host == None:
            raise Exception('The host url is required.') 
        if catalog_name == None:
            raise Exception('The catalog name is required.')
        if uid == None and pwd==None and token==None:
            raise Exception('The uid/pwd and authentication token can not be empty at the same time.')
        
        self.host = host
        self.catalog_name = catalog_name
        # retrieve auth token
        if token == None:
            self.__get_auth_token__(uid,pwd)
        else:
            self.token = token        
        # search for catalog id
        for cat in self.__get_catalogs__()['catalogs']:
            if cat['entity']['name'].lower() == catalog_name.lower():
                self.catalog_id = cat['metadata']['guid']
        # debug info
        if verbose:
            print('Initialize Waston Knowledge Catalog: sucessfully!')
            print('WKC Guid: %s'%(self.catalog_id))
    
    def get_asset_types(self):
        """
        @param::none
        return all the asset type of specified knowledge catalog
        """ 
        method = '/v2/asset_types?catalog_id=%s'%(self.catalog_id)
        data = self.__GET__(method)
        resources = self.__jsonify__(data)['resources']
        return [t['name'] for t in resources]
    
    def search_assets(self, type_name='feature_asset'):
        """
        @param string::type_name: the type name of asset in catalog
        return all asset metadata document match the asset type
        """
        if type_name == None:
            raise Exception('The asset type name is required.')
            
        method = '/v2/asset_types/%s/search?catalog_id=%s'%(type_name,self.catalog_id)
        payloads = "{\"query\":\"*:*\"}"
        data = self.__POST__(method, payloads)
        #return self.__jsonify__(data)
        return {entity['metadata']['asset_id']:entity['metadata']['name'] for entity in self.__jsonify__(data)['results']}
    
    def create_asset(self, metadata):
        """
        @param json::metadata: the stringified json object
        return response from the Waston Data API
        """
        if metadata == None:
            raise Exception('The metadata document is required.')
            
        method = '/v2/assets?catalog_id=%s'%(self.catalog_id)
        payloads = metadata
        response = self.__POST__(method, payloads)
        return self.__jsonify__(response)
    
    def checkout_asset(self, asset_id, type_name='feature_asset'):
        """
        @param string::asset_id: the guid of feature asset in catalog
        return the dumps of feature asset catalog
        """
        if asset_id == None:
            raise Exception('The asset id is required.')
            
        data = self.__get_asset_metadata__(asset_id)
        return data['entity'][type_name]['dumps']

    """
    "
    " udpate feature following RFC6902
    " e.g. [{ "op": "replace", "path": "/dumps", "value": "foo" }]
    " replace the value of node specified by path "/dumps" to "foo"
    " docu: https://cloud.ibm.com/apidocs/watson-data-api#clone-an-asset
    " docu: https://tools.ietf.org/html/rfc6902
    "
    """
    def update_asset(self, asset_id, metadata):
        """
        @param string::asset_id: the asset id.
        @param string::metadata: the stringified json object
        return the updated metadata document of specified asset
        """
        if asset_id == None:
            raise Exception('The asset id is required.')
        if metadata == None:
            raise Exception('The metadata document is required.')
        
        method = '/v2/assets/%s?catalog_id=%s'%(asset_id,self.catalog_id)
        payloads = metadata
        response = self.__PATCH__(method, payloads)
        return self.__jsonify__(response)
    
    def delete_asset(self, asset_id):
        """
        @param string:asset_id: asset id
        return None
        """
        if asset_id == None:
            raise Exception('The asset id is required.')
        method = '/v2/assets/%s?catalog_id=%s'%(asset_id,self.catalog_id)
        response = self.__DELETE__(method)
        return response
    
    def create_asset_type(self, name, metadata):
        """
        @param string::name: the name of asset type
        @param string::metadata: the stringified json object
        return the metadata document of created asset
        """
        if metadata == None:
            raise Exception('The asset type metadata document is required.')
        if name == None:
            raise Exception('The asset type name is required.')
            
        method = '/v2/asset_types/%s?catalog_id=%s'%(name,self.catalog_id)
        payloads = metadata
        response = self.__PUT__(method, payloads)
        return self.__jsonify__(response)

    """
    "
    " get attachment of the asset in catalog. it takes 4 steps(api calls) to download the attachments
    " docu: https://cloud.ibm.com/apidocs/watson-data-api#get-asset
    "
    """
    def download_attachment(self, asset_id):
        """
        """
        if self.token == None:
            raise Exception('Authentication token is required.')
            
        url = self.__get_attachment_url__(asset_id)
        if url:
            conn = http.client.HTTPSConnection(
                  self.host,
                  context = ssl._create_unverified_context()
            )
            headers = {
                'authorization': 'Bearer %s'%(self.token),
                'cache-control': 'no-cache'
            }
            conn.request("GET", url, headers=headers)
            res = conn.getresponse()
            return res.read() 
    
    """
    "
    " the asset metadata document has three major section: metadata, enitity and attachment.
    " the attributes are under entity section and customizable.
    " docu: https://cloud.ibm.com/apidocs/watson-data-api#assets
    "
    """
    def get_attribute(self, asset_id, type_name='feature_asset'):
        """
        @param string::asset_id: asset id
        @param string::name: the name of asset type
        return the specified attribute value
        """
        if asset_id == None:
            raise Exception('the asset id is required.')
        method = '/v2/assets/%s/attributes/%s?catalog_id=%s'%(asset_id,type_name,self.catalog_id)
        response = self.__GET__(method)
        return self.__jsonify__(response)
    
    def update_attribute(self, asset_id, ops, type_name='feature_asset'):
        """
        @param string::asset_id: asset id
        @param string::ops: the stringified json object based on RFC6902
        @param string::name: the name of feature type
        return the updated attributes
        """
        if asset_id == None:
            raise Exception('The asset id is required.')
        if ops == None:
            raise Exception('The patch operation data is required.')
            
        method = '/v2/assets/%s/attributes/%s?catalog_id=%s'%(asset_id, type_name, self.catalog_id)
        payloads = ops
        response = self.__PATCH__(method, payloads)
        return self.__jsonify__(response)
    
    """
    "
    " get the attachment url of given asset
    " docu: https://cloud.ibm.com/apidocs/watson-data-api#get-asset
    " docu: https://cloud.ibm.com/apidocs/watson-data-api#get-an-attachment
    "
    """
    def __get_attachment_url__(self, asset_id):
        """
        @param string::asset_id: asset id
        return the attachment download url
        """
        if asset_id == None:
            raise Exception('the asset id is required.')
        
        # get asset metadata
        metadata = self.__get_asset_metadata__(asset_id)
        attachment_id = None
        if 'attachments' in metadata:
            for m in metadata['attachments']:
                if m['asset_type'] == 'data_asset':
                    attachment_id = m['id']
                    break
        # get attachment id
        if attachment_id == None:
            return None
        else:
            method = '/v2/assets/%s/attachments/%s?catalog_id=%s'%(asset_id,attachment_id,self.catalog_id)
            data = self.__GET__(method)
            return self.__jsonify__(data)['url']
            
    """
    "
    " get the metadata of a asset
    " docu: https://cloud.ibm.com/apidocs/watson-data-api#get-an-asset
    "
    """ 
    def __get_asset_metadata__(self, asset_id):
        """
        @param string::asset_id: the asset id
        return the asset major metadata document
        """
        if asset_id == None:
            raise Exception('the asset id is required.')
            
        method = '/v2/assets/%s?catalog_id=%s'%(asset_id,self.catalog_id)
        data = self.__GET__(method)
        return self.__jsonify__(data)
    
    """
    "
    " authenicate user by username and password and get the authentication token 
    " docu: https://cloud.ibm.com/apidocs/watson-data-api#creating-an-iam-bearer-token
    "
    """
    def __get_auth_token__(self, uid, pwd, verbose=False):
        """
        @param::uid: username
        @param::pwd: password
        return authentication token
        """
        if uid == None or pwd == None:
            raise Exception('the username and password are both required.')
            
        conn = http.client.HTTPSConnection(
              self.host,
              context = ssl._create_unverified_context()
        )
        
        headers = {
            'content-type': "application/json",
            'cache-control': "no-cache",
        }
        method = '/icp4d-api/v1/authorize'
        payloads = "{\"username\":\"%s\",\"password\":\"%s\"}"%(uid,pwd) 
        
        conn.request("POST", method, payloads, headers)
        res = conn.getresponse()
        data = res.read().decode("utf-8")
        self.token = self.__jsonify__(data)['token']
        return self.token
            
        
    def __get_catalogs__(self, verbose=False):
        """
        @param::verbose: boolean value toggle debug output
        return json object contains a list of catalog object
        """
        method = '/v2/catalogs'
        data = self.__GET__(method)
        return self.__jsonify__(data)
    
    def __GET__(self, method, headers=None):
        """
        @param string:: method: the API method
        @param dict:: header: the http GET request header
        return the response data
        """
        if self.token == None:
            raise Exception('Authentication token is required.')
            
        if method == None:
            raise Exception('The API method is required.')
            
        conn = http.client.HTTPSConnection(
              self.host,
              context = ssl._create_unverified_context()
        )
        
        if headers == None:
            headers = {
                'authorization': 'Bearer %s'%(self.token),
                'cache-control': 'no-cache',
                'accept': 'application/json',
                'content-type': 'application/json'
            }
        
        
        conn.request("GET", method, headers=headers)
        res = conn.getresponse()
        return res.read().decode("utf-8")
    
    def __DELETE__(self, method, headers=None):
        """
        @param string:: method: the API method
        @param dict:: header: the http GET request header
        return the response data
        """
        if self.token == None:
            raise Exception('Authentication token is required.')
            
        if method == None:
            raise Exception('The API method is required.')
            
        conn = http.client.HTTPSConnection(
              self.host,
              context = ssl._create_unverified_context()
        )
        
        if headers == None:
            headers = {
                'authorization': 'Bearer %s'%(self.token),
                'cache-control': 'no-cache',
                'accept': 'application/json',
                'content-type': 'application/json'
            }
        
        
        conn.request("DELETE", method, headers=headers)
        res = conn.getresponse()
        return res.read().decode("utf-8")
    
    def __POST__(self, method, payloads=None, headers=None):
        """
        @param string:: method: the method API
        @param dict:: payloads: the payload of POST request
        @param dict:: headers: the header of POST request
        @return string:: the decoded response content
        """
        
        if self.token == None:
            raise Exception('Authentication token is required.')
            
        if method == None:
            raise Exception('The API method is required.')
            
        conn = http.client.HTTPSConnection(
              self.host,
              context = ssl._create_unverified_context()
        )
        
        if headers == None:
            headers = {
                'authorization': 'Bearer %s'%(self.token),
                'cache-control': "no-cache",
                'accept': 'application/json',
                'content-type': 'application/json'
                }
            
        conn.request("POST", method, payloads, headers)
        res = conn.getresponse()
        data = res.read().decode("utf-8")
        return data
    
    def __PUT__(self, method, payloads=None, headers=None):
        """
        @param string:: method: the method API
        @param dict:: payloads: the payload of POST request
        @param dict:: headers: the header of POST request
        @return string:: the decoded response content
        """
        
        if self.token == None:
            raise Exception('Authentication token is required.')
            
        if method == None:
            raise Exception('The API method is required.')
            
        conn = http.client.HTTPSConnection(
              self.host,
              context = ssl._create_unverified_context()
        )
        
        if headers == None:
            headers = {
                'authorization': 'Bearer %s'%(self.token),
                'cache-control': "no-cache",
                'accept': 'application/json',
                'content-type': 'application/json'
                }
            
        conn.request("PUT", method, payloads, headers)
        res = conn.getresponse()
        data = res.read().decode("utf-8")
        return data
    
    def __PATCH__(self, method, payloads=None, headers=None):
        """
        @param string:: method: the method API
        @param dict:: payloads: the payload of POST request
        @param dict:: headers: the header of POST request
        @return string:: the decoded response content
        """
        
        if self.token == None:
            raise Exception('Authentication token is required.')
            
        if method == None:
            raise Exception('The API method is required.')
            
        conn = http.client.HTTPSConnection(
              self.host,
              context = ssl._create_unverified_context()
        )
        
        if headers == None:
            headers = {
                'authorization': 'Bearer %s'%(self.token),
                'cache-control': "no-cache",
                'accept': 'application/json',
                'content-type': 'application/json'
                }
            
        conn.request("PATCH", method, payloads, headers)
        res = conn.getresponse()
        data = res.read().decode("utf-8")
        return data
        
    
    def __jsonify__(self, dumps):
        """
        @param::dumps: json dumps in string
        return json object
        """
        dumps_io = StringIO(dumps)
        return json.load(dumps_io)
        


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
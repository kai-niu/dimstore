"""
"  
" IBM WKC Meta Manager
"
"""
import os
import pickle as pl
import base64
import json
from nebula.providers.meta_manager.meta_manager_base import MetaManagerBase
from nebula.core.feature_set import FeatureSet
from nebula.utility.waston_knowledge_catalog_client import WastonKnowledgeCatalogClient


class WastonKnowledgeCatalogMetaManager(MetaManagerBase):

    def __init__(self, config):
        self.config = config
        self.catalog_name = config['catalog_name']
        self.asset_name = config['asset_name']
        self.uid = config['uid']
        self.pwd = config['token']
        self.host = config['host']
        self.asset_type = 'feature_manager_asset'
        self.client = self.__get_wkc_client__()

        # create feature meta asset type
        asset_types = self.client.get_asset_types()
        if self.asset_type.lower() not in asset_types:
            self.__init_manager_asset_type__()

        # get mananger asset guid
        self.asset_guid = self.__get_manager_asset_guid__()


    def is_unique(self, feature):
        self.__apply_default_namespace__(feature)
        return self.__check_feature_name_uniqueness__(feature)    

    def register(self, feature):
        """
        @param feature_metadata::feature: the feature meta data object
        return none
        """
        # handle edge case
        self.__apply_default_namespace__(feature)
        if not self.__check_feature_name_uniqueness__(feature):
            print("> the feature name: '%s' is not unique in namespace: %s" % (feature.name, feature.namespace))
            return
        # get catalog object and register feature
        uid = str(feature.uid)
        namespace = feature.namespace
        catalog = self.__get_catalog__(namespace = namespace)
        catalog[uid] = feature
        # write back catalog
        self.__save_catalog__(catalog, namespace = namespace)


    """
    "
    " read feature meta data by given namespace
    "
    """
    def read(self, namespace='default', match_child=True, **kwargs):
        """
        @param::namespace: the namespace in string
        @param::match_child: boolean value indicate whether match sub namespaces
        @param::kwargs: the keyword parameter lists
        return a dictionary of {uid:feature meta data}
        """
        feature_dict = {}
        bytes_obj = self.__read_dumps__()
        if bytes_obj:
            catalog = pl.loads(bytes_obj)
            canonical_ns = self.__build_canonical_namespace__(namespace)
            if match_child:
                for key_ns in catalog:
                    if self.__namespace_match__(canonical_ns, key_ns):
                        feature_dict.update(catalog[key_ns])
            else:
                if canonical_ns in catalog:
                    feature_dict = catalog[canonical_ns]
        return feature_dict

    """
    "
    " get all namespaces in feature store
    "
    """
    def namespaces(self, **kwargs):
        """
        @param::kwargs: keyword parameter list
        return list of canonical namespace objects and the counts of features in that namespace
        """
        catalog = {}
        bytes_obj = self.__read_dumps__()
        if bytes_obj:
            catalog = pl.loads(bytes_obj)
        return catalog.keys(),list(map(len,catalog.values()))

    """
    "
    " delete features in the given FeatureSet instance
    "
    """
    def delete(self, ufd, verbose=True, **kwargs):
        """
        @param::ufd: the instance of dictionary contain uid:feature pairs
        @param::kwargs: keyword parameter list
        return none
        """
        # check edge case
        if not isinstance(ufd, dict):
            raise ValueError('> delete: feature_set parameter is not an instance of FeatureSet class.')
        namespace = None
        catalog = None
        for uid, feature in ufd.items():
            # sanity check of feature meta data
            self.__apply_default_namespace__(feature)
            # load new catalog when namespace mismatch
            if namespace != feature.namespace:
                # save existing catalog
                if isinstance(catalog, dict):
                    self.__save_catalog__(catalog, namespace=namespace)
                # load new catalog in different namespace
                catalog = self.__get_catalog__(namespace=feature.namespace)
                namespace = feature.namespace
            # delete feature
            if isinstance(catalog, dict) and uid in catalog:
                del catalog[uid]
        # save the last catalog back into storage
        if isinstance(catalog, dict):
            self.__save_catalog__(catalog, namespace=namespace)
        if verbose:
            print('> delete: %d feature(s) affected.'%(len(ufd)))
        

    """
    "
    " update features in the given FeatureSet instance
    "
    """
    def update(self, ufd, verbose=True, **kwargs):
        """
        @param::ufd: the instance of dictionary contain uid:feature pairs
        @param::kwargs: keyword parameter list
        return none
        """
        # check edge case
        if not isinstance(ufd, dict):
            raise ValueError('> update: feature_set parameter is not an instance of FeatureSet class.')
        namespace = None
        catalog = None
        for uid, feature in ufd.items():
            # sanity check of feature meta data
            self.__apply_default_namespace__(feature)
            if not self.__check_feature_name_uniqueness__(feature):
                raise ValueError('> update: feature name "%s" is not unique in namespace "%s"'%(feature.name, feature.namespace))
            # load new catalog when namespace mismatch
            if namespace == None or namespace != feature.namespace:
                # save existing catalog
                if isinstance(catalog, dict) and len(catalog) > 0:
                    self.__save_catalog__(catalog, namespace=namespace)
                # load new catalog in different namespace
                catalog = self.__get_catalog__(feature.namespace)
                namespace = feature.namespace
            # update feature
            if uid in catalog:
                catalog[uid] = feature
                # may cause problem when manager and feature assets are in
                # different catalogs
                self.__update_feature_asset__(feature)
        # save the last catalog back into storage
        if isinstance(catalog, dict) and len(catalog) > 0:
            self.__save_catalog__(catalog, namespace=namespace)
        if verbose:
            print('> update: %d feature(s) affected.'%(len(ufd)))

    

    """
    "
    " implementation of base class method.
    " operation takes O(N), where N is the max of number of features in any namespace.
    "
    """
    def __check_feature_name_uniqueness__(self, feature):
        """
        @param::feature: the feature metadata object.
        return boolean value of uniqueness of feature name in given namespace.
        """
        bytes_obj = self.__read_dumps__()
        if bytes_obj:
            catalog = pl.loads(bytes_obj)
            # check uniquess
            feature_name = feature.name
            namespace = feature.namespace
            uid = feature.uid
            canonical_ns = self.__build_canonical_namespace__(namespace)
            if  canonical_ns in catalog:
                    for _,feature in catalog[canonical_ns].items():
                        if feature.name.lower() == feature_name.lower() and feature.uid != uid:
                            return False
        return True

    """
    "
    " read the catalog dumps and deserialize it
    " *** <note> may have performance issue when request/s is high, a cache can address this problem.
    " *** general cache mechanism: cache catalog using namespace as key, destroy/update when cached catalog with same namespace is updated.
    "
    """
    def __get_catalog__(self, namespace=None):
        """
        @param::namespace: the string namespace of catalog
        return the catalog object or None
        """
        catalog_rtn = {}
        namespace = self.__build_canonical_namespace__(namespace)
        bytes_obj = self.__read_dumps__()
        if bytes_obj:
            catalog = pl.loads(bytes_obj)
            if namespace in catalog:
                catalog_rtn = catalog[namespace]        
        return catalog_rtn

    """
    "
    " serialize the catalog and save it to flat file
    "
    """
    def __save_catalog__(self, sub_catalog, namespace=None):
        """
        @param::sub_catalog: the catelog object contains feature metadata
        @param::namespace: the string namespace of catalog
        return None
        """
        # check edge case
        if sub_catalog == None:
            raise Exception('> save_catalog: (sub)catalog can not be None!')
        if not isinstance(sub_catalog, dict):
            raise Exception('> save_catalog: (sub)catalog is not dictionary type!')
        # save (sub)catalog into flat file
        catalog = {}
        namespace = self.__build_canonical_namespace__(namespace)
        bytes_obj = self.__read_dumps__()
        if bytes_obj:
            catalog = pl.loads(bytes_obj)
            if len(sub_catalog) == 0:
                # delete the namespace associate with empty sub catalog.
                del catalog[namespace]
            else:
                catalog[namespace] = sub_catalog
        else:
            if len(sub_catalog) > 0:
                catalog[namespace] = sub_catalog
        dumps = pl.dumps(catalog)
        self.__udpate_dumps__(dumps)


    """
    "
    " method apply default namespace if given namespace is invalid
    " 
    """
    def __apply_default_namespace__(self, feature):
        """
        @param::feature: the instance of feature metadata class
        return none
        """
        if feature.namespace == None or feature.namespace.strip() == '':
            feature.namespace = 'default'


    """
    "
    " build a set of canonical namespaces optimized for query/filter operation
    " e.g., foo.bar.kai => {(foo,0),(bar,1),(kai,2)}
    " check whether or not namepace match take O(N), where N is the number of tuples in namespace.
    "
    """
    def __build_canonical_namespace__(self, namespace):
        """
        @param::namespace: string representation of namespace.
        return a set contains canonical namespace
        """
        canonical_ns = []
        existed_parts = set([])
        if namespace == None or namespace.strip() == '':
            canonical_ns.append((0,'default'))
        else:
            for index,part in enumerate(namespace.split('.')):
                if not part.isalnum():
                    raise Exception("> The namespace is invalid! Make sure the namespace contains alphanumeric and '.' symbol only.")
                if part.lower() in existed_parts:
                    raise Exception("> The namespace can not contains duplicated tuples!")
                else:
                    canonical_ns.append((index,part.lower()))
                    existed_parts.add(part.lower())

        return tuple(canonical_ns)

    """
    "
    " check if the canonical namespaces match. The match operation is not communicative.
    " e.g., a match b  !=> b match a
    " operation takes O(N), where N is the number of tuples in namespace
    "
    """
    def __namespace_match__(self,namespace1, namespace2):
        if namespace1 == None or namespace2 == None:
            return False
        for part in namespace1:
            if part not in namespace2:
                return False
        return True

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

    def __read_dumps__(self):
        """
        @param::none
        return the byte dumps of feature manager asset
        """
        content = None
        try:
            dumps = self.client.checkout_asset(self.asset_guid, self.asset_type)
            content = self.__decode__(dumps)
        except Exception as e:
            print('> ibm WKC client read feature asset failed! \n',e)
            raise
        return content

    def __get_manager_asset_guid__(self):
        """
        @param::none
        return the guid id of feature manager asset specified name
        """
        assets = self.client.search_assets(type_name=self.asset_type)
        if len(assets) == 0:
            return self.__init_manager_asset__()
        else:
            # O(N) search, possible to pushdown the query to API to achieve
            # better performance. Currently design can only result few dups.
            for uid,name in assets.items():
                if name.lower() == self.asset_name.lower():
                    return uid
            return self.__init_manager_asset__()

    def __init_manager_asset_type__(self):
        """
        @param::none
        return none
        """
        asset_type = {
                "description": "DimStore feature manager asset",
                "fields": [
                {
                    "key": "name",
                    "type": "string",
                    "facet": False,
                    "is_array": False,
                    "search_path": "name",
                    "is_searchable_across_types": False
                }
            ]
            }
        try:
            self.client.create_asset_type(self.asset_type,json.dumps(asset_type))
        except Exception as e:
            print('> WKC Meta Manager create asset type failed.', e)
            raise

    def __init_manager_asset__(self):
        """
        @param::none
        return the guid of newly created manager asset.
        """
        byte_obj = pl.dumps({})
        dumps = self.__encode__(byte_obj)
        # define the features 
        asset = {    
                    "metadata": {
                            "name": self.asset_name,
                            "description": "DimStore feature metadata manager asset.",
                            "tags": ["DimStore"],
                            "asset_type": self.asset_type,
                            "origin_country": "us",
                            "rov": {
                            "mode": 0
                            }
                    },
                    "entity": {
                        self.asset_type:{
                            "name": self.asset_name,
                            "dumps": dumps
                        }
                    }
                }
        # create feature
        response = self.client.create_asset(json.dumps(asset))
        return response['metadata']['asset_id']

    """
    "
    " update feature asset metadata
    "
    """
    def __update_feature_asset__(self, feature):
        """
        @param FeatureMetaData::feature: the feature meta data object.
        return none
        """
        # define update operations
        ops =  [
            { "op": "replace", "path": "/metadata/tags", "value": list(feature.tags)},
            { "op": "replace", "path": "/metadata/description", "value": feature.comment},
        ]
        # update feature asset metadata
        try:
            self.client.update_asset(feature.uid,json.dumps(ops))
        except Exception as e:
            print('> update feature asset metadata failed.', e)

    """
    "
    " update the byte dumps of manager asset
    "
    """
    def __udpate_dumps__(self, dumps):
        """
        @param string::dumps: stringified json metadata object
        return none
        """
        str_dumps = self.__encode__(dumps)
        ops =  [{ "op": "replace", "path": "/dumps", "value": str_dumps }]
        self.client.update_attribute(self.asset_guid,json.dumps(ops),type_name=self.asset_type)

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



                    







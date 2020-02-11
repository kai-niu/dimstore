"""
"  
" IBM WKC Meta Manager
"
"""
import os
import pickle as pl
import base64
import json
import ibm_boto3
import warnings
from ibm_botocore.errorfactory import ClientError
from botocore.client import Config
from nebula.providers.meta_manager.meta_manager_base import MetaManagerBase
from nebula.core.feature_set import FeatureSet

class IBMObjectStorageMetaManager(MetaManagerBase):

    def __init__(self, config):
        self.config = config
        self.IAM_SERVICE_ID = config['iam_service_id']
        self.IBM_API_KEY_ID = config['ibm_api_key_id']
        self.ENDPOINT = config['endpoint']
        self.IBM_AUTH_ENDPOINT = config['ibm_auth_endpoint']
        self.BUCKET = config['bucket']
        self.meta_filename = config['object_name']
        self.client = self.__get_boto_client__()


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
        self.__write_dumps__(dumps)


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

    def __get_boto_client__(self):
        """
        @param::none:
        return the ibm boto client instance
        """
        client = None
        try:
            client = ibm_boto3.client(service_name='s3',
                                    ibm_api_key_id=self.IBM_API_KEY_ID,
                                    ibm_auth_endpoint=self.IBM_AUTH_ENDPOINT,
                                    config=Config(signature_version='oauth'),
                                    endpoint_url=self.ENDPOINT)
        except Exception as e:
            print('> ibm boto client initialization failed! \n', e)

        return client

    def __read_dumps__(self):
        """
        @param::none
        return the byte dumps of feature manager asset
        """
        filename = '%s.dill'%(self.meta_filename)
        content = None
        try:
            body = self.client.get_object(Bucket=self.BUCKET,Key=filename)['Body']
            content = body.read()
        except ClientError as ce:
            if ce.response['Error']['Code'] == 'NoSuchKey':
                dumps = pl.dumps({})
                self.__write_dumps__(dumps)
                print('> ibm boto meta_manager file does not exist, create new one for you :)')
            else:
                print('> ibm boto client error: ', ce)
        except Exception as e:
                print('> ibm boto read operation failed! \n',e)

        return content


    """
    "
    " update the byte dumps of manager asset
    "
    """
    def __write_dumps__(self, dumps):
        """
        @param string::dumps: stringified json metadata object
        return none
        """
        filename = '%s.dill'%(self.meta_filename)
        try:
            self.client.put_object(ACL='public-read', Body=dumps, Bucket=self.BUCKET, Key=filename)
        except Exception as e:
            print('> ibm boto upload operation failed!', e)



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



                    







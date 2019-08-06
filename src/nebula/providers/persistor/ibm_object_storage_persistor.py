"""
    IBM Objerct Storage persistor class abstracts the I/O operations to flat file.
    IBM boto3 API document: https://ibm.github.io/ibm-cos-sdk-python/index.html
    botocore API document: https://botocore.amazonaws.com/v1/documentation/api/latest/tutorial/index.html
"""
from nebula.providers.persistor.persistor_base import PersistorBase
from botocore.client import Config
import ibm_boto3

class IBMObjectStoragePersistor(PersistorBase):
    def __init__(self, config):
        self.config = config
        self.IAM_SERVICE_ID = 'iam-ServiceId-1915183a-47f4-4c9f-81c3-a246ba40d916'
        self.IBM_API_KEY_ID = '4r8w7hJilAQyo4VrdBqUnhbXA5qfratq25FP1IsgvKh_'
        self.ENDPOINT = 'https://s3.us.cloud-object-storage.appdomain.cloud'
        self.IBM_AUTH_ENDPOINT = 'https://iam.bluemix.net/oidc/token'
        self.BUCKET = 'foobar-bucket'
        self.client = self.__get_boto_client__()


    def write(self, feature, dumps, **kwargs):
        """
        @param::feature: instance of Feature class
        @param::dumps: the byte codes of pipeline dumps
        @param::kwargs: name parameter list
        return none
        """
        # upload to object storage
        filename = '%s.dill'%(feature.uid)
        try:
            self.client.put_object(ACL='public-read', Body=dumps, Bucket=self.BUCKET, Key=filename)
        except Exception as e:
            print('> ibm boto upload operation failed!', e)
     
  

    def read(self, uid, **kwargs):
        """
        @param::uid: symbolic string name used to identify the feature
        @param::kwargs: named parameter list
        return the content of the file
        """
        # client to access IBM Object Storage
        filename = '%s.dill'%(uid)
        try:
            body = self.client.get_object(Bucket=self.BUCKET,Key=filename)['Body']
        except Exception as e:
            print('> ibm boto read operation failed! \n',e)
        return body

    def delete(self, uid, **kwargs):
        """
        @param::uid: symbolic string name used to identify the feature
        @param::kwargs: named parameter list
        return none
        """
        # client to access IBM Object Storage
        filename = '%s.dill'%(uid)
        try:
            self.client.delete_object(Bucket=self.BUCKET,Key=filename)
        except Exception as e:
            print('> ibm boto delete operation failed! \n',e)


    def __get_boto_client__(self):
        """
        @param::none:
        return the ibm boto client instance
        """
        client = ibm_boto3.client(service_name='s3',
                                  ibm_api_key_id=self.IBM_API_KEY_ID,
                                  ibm_auth_endpoint=self.IBM_AUTH_ENDPOINT,
                                  config=Config(signature_version='oauth'),
                                  endpoint_url=self.ENDPOINT)
        return client
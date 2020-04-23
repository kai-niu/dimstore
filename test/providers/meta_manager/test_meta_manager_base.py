"""
    test meta data manager base class
"""
import pytest
from dimstore.providers.meta_manager.meta_manager_base import MetaManagerBase

@pytest.fixture(scope='function')
def base_class(request):
    return MetaManagerBase(None)

class TestMetaManagerBase():

    def test_register_method_exception(self, base_class):
        with pytest.raises(NotImplementedError):
            base_class.register(None)

    def test_lookup_method_exception(self, base_class):
        with pytest.raises(NotImplementedError):
            base_class.lookup(None)

    def test_list_features_method_exception(self, base_class):
        with pytest.raises(NotImplementedError):
            base_class.list_features()

    def test_inpsect_feature_method_exception(self, base_class):
        with pytest.raises(NotImplementedError):
            base_class.inspect_feature(None)

    def test_remove_feature_method_exception(self, base_class):
        with pytest.raises(NotImplementedError):
            base_class.remove_feature(None)

    
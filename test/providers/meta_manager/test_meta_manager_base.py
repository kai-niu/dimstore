"""
    test meta data manager base class
"""
import pytest
from nebula.providers.meta_manager.meta_manager_base import MetaManagerBase

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

    def test_build_canonical_namespace_method_default_namespace(self, base_class):
        # arrange
        manager = base_class
        value_list = [None,'','       ']
        # act
        result_list = [manager.__build_canonical_namespace__(val) for val in value_list]
        # assert
        for val in result_list:
            assert val == ((0,'default'),)

    def test_build_canonical_namespace_method_raise_exception(self, base_class):
        # arrange
        value_list = ['.','..',' . . . ','a-.bc','foo.foo','foo.bar.foo']
        # act/assert
        for val in value_list:
            with pytest.raises(Exception):
                base_class.__build_canonical_namespace__(val)

    def test_build_canonical_namespace_method_expected_result(self, base_class):
        # arrange
        value_list = ['foo.bar','default.foo.Bar']
        expected_namespace_list = [
            ((0,'foo'),(1,'bar')),
            ((0,'default'),(1,'foo'),(2,'bar'))
        ]
        # act
        namespace_list = [base_class.__build_canonical_namespace__(val) for val in value_list]
        # assert
        for index, ns in enumerate(expected_namespace_list):
            assert len(ns) == len(namespace_list[index])
            for part in ns:
                assert part in namespace_list[index]

    def test_namespace_match_method_expected_false_return(self, base_class):
        # arrange
        ns1_list = [
            None,
            ((0,'foo'),(1,'bar')),
            ((0,'foo'),(1,'bar')), 
            ((0,'foo'))
            ]
        ns2_list = [
            ((0,'foo'),(1,'bar')),
            None,
            ((0,'foo')),
            ((0,'bar'),(1,'foo'))
            ]
        # act
        result_list = [base_class.__namespace_match__(ns1_list[index],ns2_list[index]) for index in range(len(ns1_list))]
        # assert
        for result in result_list:
            assert result == False

    def test_namespace_match_method_expected_true_return(self, base_class):
        # arrange
        ns1_list = [
            ((0,'foo'),),
            ((0,'bar'),(1,'foo')),
            ((0,'default'),)
            ]
        ns2_list = [
            ((0,'foo'),(1,'bar')),
            ((0,'bar'),(1,'foo')),
            ((0,'default'),(1,'foo'))
        ]
        # act
        result_list = [base_class.__namespace_match__(ns1_list[index],ns2_list[index]) for index in range(len(ns1_list))]
        # assert
        for result in result_list:
            assert result == True


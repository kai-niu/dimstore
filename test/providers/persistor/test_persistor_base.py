"""
    test persistor base class
"""

import pytest
from nebula.providers.persistor.persistor_base import PersistorBase


@pytest.fixture(scope='function')
def base_class(request):
    return PersistorBase()


class TestCacheLayerBase():

    def test_put_method_implementation_exception(self, base_class):
        with pytest.raises(NotImplementedError):
            base_class.write(None,None)

    def test_get_method_implementation_exception(self, base_class):
        with pytest.raises(NotImplementedError):
            base_class.read(None)

    def test_stats_method_implementation_exception(self, base_class):
        with pytest.raises(NotImplementedError):
            base_class.delete(None)
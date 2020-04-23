"""
    test cache base class
"""

import pytest
from dimstore.providers.cache.cache_base import CacheLayerBase


@pytest.fixture(scope='function')
def base_class(request):
    return CacheLayerBase()


class TestCacheLayerBase():

    def test_put_method_implementation_exception(self, base_class):
        with pytest.raises(NotImplementedError):
            base_class.put(None,None)

    def test_get_method_implementation_exception(self, base_class):
        with pytest.raises(NotImplementedError):
            base_class.get(None)

    def test_stats_method_implementation_exception(self, base_class):
        with pytest.raises(NotImplementedError):
            base_class.stats()



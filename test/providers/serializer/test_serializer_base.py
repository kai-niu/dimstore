"""
    test serializer base class
"""

import pytest
from nebula.providers.serializer.serializer_base import SerializerBase


@pytest.fixture(scope='function')
def base_class(request):
    return SerializerBase()


class TestSerializerBase():

    def test_put_method_implementation_exception(self, base_class):
        with pytest.raises(NotImplementedError):
            base_class.encode(None)

    def test_get_method_implementation_exception(self, base_class):
        with pytest.raises(NotImplementedError):
            base_class.decode(None)

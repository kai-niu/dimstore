"""
    test utility file functions
"""
import pytest
from unittest import mock
from nebula.utility.file_functions import parse_file_protocol, parse_file_uri, http_read_file


@pytest.fixture(scope='function')
def mock_uri(request):
    # mock the config object
    uri_list = {
                'http': ('http://www.foobar.com/foo.bar', 'http'),
                'https': ('https://www.foobar.com/foo.bar', 'https'),
                'file': ('file://./foobar/foo.bar', 'file'),
                'none': (None, None),
                'empty': (' ', None)
    }
    return uri_list

@pytest.fixture(scope='function')
def mock_file_uri(request):
    # mock the config object
    uri_list = {
        'file://~/foo.json':('~','foo.json'),
        'file://C:/foo/bar/foobar.json':('c:/foo/bar','foobar.json'),
        'http://www.foobar.com/foobar.json':(None,None),
        'file://foobar.json':('.','foobar.json'),
        'file://':('.',''),
        '':(None,None),
        None:(None,None)

    }
                
    return uri_list

class TestUtilityFileFunctions():

    def test_parse_file_protocol_result(self, mock_uri):
        for _,val in mock_uri.items():
            # arrange
            uri, expected_protocal = val
            # act
            portocol = parse_file_protocol(uri)
            # assert
            assert portocol == expected_protocal

    def test_parse_file_uri_result(self, mock_file_uri):
        for uri,expected_result in mock_file_uri.items():
            # arrange
            pass
            # act
            dirname, filename = parse_file_uri(uri)
            # assert
            assert expected_result[0] == dirname
            assert expected_result[1] == filename

    def test_http_read_file_success(self):
        with mock.patch('nebula.utility.file_functions.urllib.request') as mock_request:
            # arrange
            mock_response = mock.Mock()
            mock_response.read.return_value = 'test content'
            mock_request.urlopen.return_value = mock_response
            # act
            content = http_read_file('', retry_interval=0.01)
            # assert
            assert content == 'test content'

    def test_http_read_file_failed_and_retry(self):
        with mock.patch('nebula.utility.file_functions.urllib.request') as mock_request:
            # arrange
            mock_request.urlopen.side_effect = Exception('Test Exception')
            # act
            max_retry = 10
            http_read_file('', max_retry=max_retry, retry_interval=0.01)
            # assert
            assert mock_request.urlopen.call_count == max_retry
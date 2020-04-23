"""
    test utility file functions
"""
import pytest
from unittest import mock
from dimstore.utility.file_functions import *


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
        with mock.patch('dimstore.utility.file_functions.urllib.request') as mock_request:
            # arrange
            mock_response = mock.Mock()
            mock_response.read.return_value = 'test content'
            mock_request.urlopen.return_value = mock_response
            # act
            content = http_read_file('', retry_interval=0.01)
            # assert
            assert content == 'test content'
    
    def test_http_read_file_success_after_k_attempts(self):
        with mock.patch('dimstore.utility.file_functions.urllib.request') as mock_request:
            # arrange
            mock_response = mock.Mock()
            mock_response.read.return_value = 'test content'
            mock_response.read.side_effect = [Exception("First"), Exception("Second"), mock.DEFAULT]
            mock_request.urlopen.return_value = mock_response
            # act
            content = http_read_file('', retry_interval=0.01)
            # assert
            assert mock_response.read.call_count == 3 and content == 'test content'

    def test_http_read_file_failed_and_retry(self):
        with mock.patch('dimstore.utility.file_functions.urllib.request') as mock_request:
            # arrange
            mock_request.urlopen.side_effect = Exception('Test Exception')
            # act
            max_retry = 10
            http_read_file('', max_retry=max_retry, retry_interval=0.01)
            # assert
            assert mock_request.urlopen.call_count == max_retry

    def test_file_exist_invalid_file(self):
        with mock.patch('os.path') as mock_request:
            # arrange
            mock_request.isfile.return_value = False
            mock_request.exists.return_value = True
            # act
            exist = file_exist('invalid/filepath', 'foo')
            # assert
            assert exist == False
    
    def test_file_exist_invalid_path(self):
        with mock.patch('os.path') as mock_request:
            # arrange
            mock_request.isfile.return_value = True
            mock_request.exists.return_value = False
            # act
            exist = file_exist('invalid/filepath', 'foo')
            # assert
            assert exist == False

    def test_file_exist_valid_file(self):
        with mock.patch('os.path') as mock_request:
            # arrange
            mock_request.isfile.return_value = True
            mock_request.exists.return_value = True
            # act
            exist = file_exist('invalid/filepath', 'foo')
            # assert
            assert exist == True

    def test_try_read_file_success(self):
        with mock.patch('dimstore.utility.file_functions.file_exist') as mock_file_exist:
            with mock.patch('builtins.open', mock.mock_open(read_data="foo")) as _:
                # arrange
                mock_file_exist.return_value = True
                # act
                data = try_read_file('foo','bar','w')
                # assert
                assert data == 'foo'
    
    def test_try_read_file_success_after_k_attemps(self):
        with mock.patch('dimstore.utility.file_functions.file_exist') as mock_file_exist:
            with mock.patch('builtins.open', mock.mock_open(read_data="data")) as mock_open:
                # arrange
                mock_file_exist.return_value = True
                mock_open.side_effect = [Exception("First"), Exception("Second"), mock.DEFAULT]
                max_attempts = 10
                retry_interval = 0.001
                # act
                data = try_read_file('foo','bar','w',max_retry=max_attempts,retry_interval=retry_interval)
                # assert
                assert mock_open.call_count == 3 and data == "data"

    def test_try_read_file_failed_max_try(self):
        with mock.patch('dimstore.utility.file_functions.file_exist') as mock_file_exist:
            with mock.patch('builtins.open', mock.mock_open(read_data="foo")) as mock_open:
                # arrange
                mock_file_exist.return_value = True
                mock_open.side_effect = Exception('foo error!')
                max_attempts = 10
                retry_interval = 0.001
                # act
                data = try_read_file('foo','bar','w',max_retry=max_attempts,retry_interval=retry_interval)
                # assert
                assert mock_open.call_count == max_attempts and data == None
    
    def test_try_read_file_failed_not_exist(self):
        with mock.patch('dimstore.utility.file_functions.file_exist') as mock_file_exist:
            with mock.patch('builtins.open', mock.mock_open(read_data="foo")) as mock_open:
                # arrange
                mock_file_exist.return_value = False
                # act
                data = try_read_file('foo','bar','w')
                # assert
                assert mock_open.call_count == 0 and data == None

    def test_try_write_file_success(self):
        with mock.patch('dimstore.utility.file_functions.file_exist') as mock_file_exist:
            with mock.patch('builtins.open', mock.mock_open(read_data="foo")) as _:
                # arrange
                mock_file_exist.return_value = True
                # act
                status = try_write_file('foo','bar','data','w')
                # assert
                assert status == 0
    
    def test_try_write_file_success_after_k_attemps(self):
        with mock.patch('dimstore.utility.file_functions.file_exist') as mock_file_exist:
            with mock.patch('builtins.open', mock.mock_open(read_data="foo")) as mock_open:
                # arrange
                mock_file_exist.return_value = True
                mock_open.side_effect = [Exception("First"), Exception("Second"), mock.DEFAULT]
                max_attempts = 10
                retry_interval = 0.001
                # act
                status = try_write_file('foo','bar','data','w',max_retry=max_attempts,retry_interval=retry_interval)
                # assert
                assert mock_open.call_count == 3 and status == 0

    def test_try_write_file_failed_max_try(self):
        with mock.patch('dimstore.utility.file_functions.file_exist') as mock_file_exist:
            with mock.patch('builtins.open', mock.mock_open(read_data="foo")) as mock_open:
                # arrange
                mock_file_exist.return_value = True
                mock_open.side_effect = Exception('foo error!')
                max_attempts = 10
                retry_interval = 0.001
                # act
                status = try_write_file('foo','bar','data','w',max_retry=max_attempts,retry_interval=retry_interval)
                # assert
                assert mock_open.call_count == max_attempts and status == 1

    def test_try_write_file_not_exist_not_auto_create(self):
        with mock.patch('dimstore.utility.file_functions.file_exist') as mock_file_exist:
            with mock.patch('builtins.open', mock.mock_open(read_data="foo")) as mock_open:
                # arrange
                mock_file_exist.return_value = False
                # act
                status = try_write_file('foo','bar','data','w', auto_create=False)
                # assert
                assert mock_open.call_count == 0 and status == 1

    def test_try_write_file_not_exist_with_auto_create(self):
        with mock.patch('dimstore.utility.file_functions.file_exist') as mock_file_exist:
            with mock.patch('builtins.open', mock.mock_open(read_data="foo")) as mock_open:
                # arrange
                mock_file_exist.return_value = False
                # act
                status = try_write_file('foo','bar','data','w', auto_create=True)
                # assert
                assert mock_open.call_count == 1 and status == 0

    def test_try_delete_file_success(self):
        with mock.patch('dimstore.utility.file_functions.file_exist') as mock_file_exist:
            with mock.patch('os.remove') as _:
                # arrange
                mock_file_exist.return_value = True
                # act
                status = try_delete_file('foo','bar')
                # assert
                assert status == 0

    def test_try_delete_file_success_after_k_attemps(self):
        with mock.patch('dimstore.utility.file_functions.file_exist') as mock_file_exist:
            with mock.patch('os.remove') as mock_remove:
                # arrange
                mock_file_exist.return_value = True
                mock_remove.side_effect = [Exception("First"), Exception("Second"), mock.DEFAULT]
                max_attempts = 10
                retry_interval = 0.001
                # act
                status = try_delete_file('foo','bar',max_retry=max_attempts,retry_interval=retry_interval)
                # assert
                assert mock_remove.call_count == 3 and status == 0

    def test_try_delete_file_failed_max_try(self):
        with mock.patch('dimstore.utility.file_functions.file_exist') as mock_file_exist:
            with mock.patch('os.remove') as mock_remove:
                # arrange
                mock_file_exist.return_value = True
                mock_remove.side_effect = Exception("error")
                max_attempts = 10
                retry_interval = 0.001
                # act
                status = try_delete_file('foo','bar',max_retry=max_attempts,retry_interval=retry_interval)
                # assert
                assert mock_remove.call_count == 10 and status == 1

    def test_try_delete_file_failed_not_exist(self):
        with mock.patch('dimstore.utility.file_functions.file_exist') as mock_file_exist:
            with mock.patch('os.remove') as mock_remove:
                # arrange
                mock_file_exist.return_value = False
                # act
                status = try_delete_file('foo','bar')
                # assert
                assert mock_remove.call_count == 0 and status == 1
    
    def test_read_text_file_success(self):
        with mock.patch('dimstore.utility.file_functions.try_read_file') as mock_read:
            # arrange
            mock_read.return_value = "foo"
            # act
            data = read_text_file('foo','bar')
            # assert
            assert data == "foo"

    def test_read_text_file_failed(self, capsys):
        with mock.patch('dimstore.utility.file_functions.try_read_file') as mock_read:
            # arrange
            mock_read.return_value = None
            # act/assert
            with pytest.raises(Exception): 
                read_text_file('foo','bar')
    
    def test_binary_file_success(self):
        with mock.patch('dimstore.utility.file_functions.try_read_file') as mock_read:
            # arrange
            mock_read.return_value = "foo"
            # act
            data = read_binary_file('foo','bar')
            # assert
            assert data == "foo"

    def test_read_binary_file_failed(self):
        with mock.patch('dimstore.utility.file_functions.try_read_file') as mock_read:
            # arrange
            mock_read.return_value = None
            # act/assert
            with pytest.raises(Exception):
                read_binary_file('foo','bar')

    def test_write_text_file_failed(self, capsys):
        with mock.patch('dimstore.utility.file_functions.try_write_file') as mock_write:
            # arrange
            mock_write.return_value = 1
            # act/assert
            with pytest.raises(Exception):
                write_text_file('foo','bar','data')
    
    def test_write_binary_file_failed(self, capsys):
        with mock.patch('dimstore.utility.file_functions.try_write_file') as mock_write:
            # arrange
            mock_write.return_value = 1
            # act/assert
            with pytest.raises(Exception):
                write_binary_file('foo','bar','data')

    def test_delete_file_failed(self, capsys):
        with mock.patch('dimstore.utility.file_functions.try_delete_file') as mock_delete:
            # arrange
            mock_delete.return_value = 1
            # act/assert
            with pytest.raises(Exception):
                delete_file('foo','bar','data')
            

    

    

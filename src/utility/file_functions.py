import os
import time


def file_exist(path, filename):
    file_url = os.path.abspath(path) + '/' + filename
    exist = os.path.exists(file_url) and os.path.isfile(file_url)
    return exist


def try_read_file(path, filename, mode, max_retry=10, retry_interval=1):
    exist = file_exist(path, filename)
    retry = 0
    if exist:
        file_url = os.path.abspath(path) + '/' + filename
        while(retry < max_retry):
            try: 
                with open(file_url, mode) as f:
                    data = f.read()
                return data
            except:
                retry += 1
                time.sleep(retry_interval)
                continue
    return None
            

def try_write_file(path, filename, data, mode, auto_create=True, max_retry=10, retry_interval=1):
    exist = file_exist(path, filename)
    if not exist and not auto_create:
        print("not exist.")
        return 1
    else:
        retry = 0
        file_url = os.path.abspath(path) + '/' + filename
        if auto_create: mode = mode + '+'
        while(retry < max_retry):
            try:
                with open(file_url, mode) as f:
                    f.write(data)
                return 0
            except:
                retry += 1
                time.sleep(retry_interval)
                continue
        return 1

def read_text_file(path, filename, max_retry=10, retry_interval=1):
    content = try_read_file(path, filename, 'r', max_retry, retry_interval)
    if content == None:
        raise Exception('Read text file: %s failed!' % (filename))
    return content

def read_binary_file(path, filename, max_retry=10, retry_interval=1):
    data = try_read_file(path, filename, 'rb', max_retry, retry_interval)
    if data == None:
        raise Exception('Read binary file: %s failed!' % (filename))
    return data
    

def write_text_file(path, filename, data, max_retry=10, retry_interval=1):
    status = try_write_file(path, filename, data, 'w', max_retry, retry_interval)
    if status != 0:
        raise Exception('Write text file: %s failed!' % (filename))

def write_binary_file(path, filename, data, max_retry=10, retry_interval=1):
    status = try_write_file(path, filename, data, 'wb', max_retry, retry_interval)
    if status != 0:
        raise Exception('Write binary file: %s failed!' % (filename))
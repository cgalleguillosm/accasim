"""
MIT License

Copyright (c) 2017 cgalleguillosm

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
"""
from os import makedirs as _makedir, listdir as _listdir
from os.path import isfile as _isfile, isdir as _isdir, join as _join
from ntpath import split as _split, basename as _basename
from shutil import rmtree as _rmtree
from json import dump as _dump, load as _load


def file_exists(file_path, boolean=False, raise_error=False, head_message=''):
    """

    :param file_path:
    :param boolean:
    :param raise_error:
    :param head_message:
    :return:
    """
    exists = _isfile(file_path)
    if (raise_error or head_message != '') and not exists:
        raise Exception('{}{} File does not exist.'.format(head_message, file_path))
    if boolean:
        return exists
    return file_path


def dir_exists(dir_path, create=False):
    """

    :param dir_path:
    :param create:
    :return:
    """
    exists = _isdir(dir_path)
    if create and not exists:
        _makedir(dir_path)
        return True
    return exists


def path_leaf(path):
    """

    :param path:
    :return:
    """
    """

    Extract path and filename

    :param path: Entire filepath

    :return: Return a tuple that contains the (path, filename)

    """
    head, tail = _split(path)
    return (head, tail or _basename(head))


def remove_dir(path, parent_folder='results/'):
    """

    :param path:
    :param parent_folder:
    :return:
    """
    if path.startswith(parent_folder) and dir_exists(path):
        _rmtree(path)


def find_file_by(_path, prefix=None, sufix=None):
    """

    :param _path:
    :param prefix:
    :param sufix:
    :return:
    """
    filename = None
    for _filename in _listdir(_path):
        _filepath = _join(_path, _filename)
        if _isfile(_filepath) and (_filename.startswith(prefix) if prefix else _filename.endswith(sufix)):
            if not filename:
                filename = _filename
            else:
                raise Exception('Multiple benchmark files in a same folder.')
    return filename


def save_jsonfile(filepath, _dict, **kwargs):
    """

    :param filepath:
    :param _dict:
    :param kwargs:
    :return:
    """
    """
    
    Saves a json file.
    
    :param config_fp: Filepath to the config
    :param \*\*kwargs: Source for the config data  
    
    """
    with open(filepath, 'w') as file:
        _dump(_dict, file, **kwargs)


def load_jsonfile(filepath):
    """

    :param filepath:
    :return:
    """
    """
    
    Loads an specific json file
    
    :param filepath: Filepath of a json file.
    
    :return: A dictionary 
    
    """
    with open(filepath) as file:
        return _load(file)

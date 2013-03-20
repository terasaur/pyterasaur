#
# Copyright 2012 ibiblio
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0.txt
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import os
import ConfigParser

"""
Generic ConfigHelper to aid in parsing an ini-style config file.  Uses
ConfigParser.

This class should extended with the following module variables declared:

CONFIG_FILENAME: string, name of configuration file

CONFIG_SEARCH_DEFAULT: list, series of relative and absolute paths to
    search through for finding a config file.  Should be in order
    of preference.

Any custom section names.

DEFAULT_VALUES: dict, declarations of default values.  Should be in the form
    {
        'section name 1': {
            'key': 'value',
            ...
            },
        'section name 2': {
            ...
            }
    }

"""

MAIN_SECTION = 'main'
MQ_SECTION = 'message_queue'
MONGODB_SECTION = 'mongodb'
DEFAULT_VALUES = None
DEFAULT_SEARCH = None

def set_defaults(values=None, search=None):
    """
    Set default configuration values and config file search path list.  Use
    once in the runtime lifecycle.

    @param values Default values
    @param search Default search path list
    """
    global DEFAULT_VALUES
    DEFAULT_VALUES = values
    global DEFAULT_SEARCH
    DEFAULT_SEARCH = search

class ConfigHelper(object):
    def __init__(self, **kwargs):
        """
        @param file_list Override the default file search list.
        """
        if kwargs.get('file_list'):
            self._config_file = self._find_file(kwargs.get('file_list'))
        else:
            self._config_file = self._find_file(DEFAULT_SEARCH)
        if not self._config_file:
            raise Exception('Could not find config file')
        self._config = None

    def _find_file(self, file_list):
        found = ''
        for file_path in file_list:
            if os.path.exists(file_path):
                found = os.path.realpath(file_path)
                break
        return found

    def get_config(self):
        if not self._config:
            self._config = self._read_config(self._config_file)
        return self._config

    def reset(self):
        self._config = None

    def _read_config(self, config_file):
        if not config_file:
            raise Exception('Missing config file path trying to read config')
        config = self._get_config_with_defaults()
        config.read(config_file)
        return config

    def _get_config_with_defaults(self):
        config = ConfigParser.RawConfigParser()
        for section in DEFAULT_VALUES:
            config.add_section(section)
            for k,v in DEFAULT_VALUES[section].iteritems():
                config.set(section, k, v)
        return config

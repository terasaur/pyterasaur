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

import sys
import terasaur.log.log_helper as log_helper
from terasaur.config.config_helper import MAIN_SECTION

class LogInitMixin(object):
    def __init__(self, **kwargs):
        """
        Initialize logging based on configuration and input param settings.

        Uses:
            self._forked
            self._debug
        Sets:
            self._log
        """
        settings = self._get_log_settings(kwargs.get('config', None))
        log_helper.initialize(settings['log_type'], settings['log_level'], settings['log_target'])
        self._log = log_helper.get_logger(self.__class__.__name__)

    def _get_log_settings(self, config):
        if self._forked:
            settings = self._get_log_settings_normal(config)
        else:
            settings = self._get_log_settings_foreground(config)

        if self._debug:
            settings['log_level'] = 'debug'
        else:
            settings['log_level'] = config.get(MAIN_SECTION, 'log_level')
        return settings

    def _get_log_settings_normal(self, config):
        # Log according to config file settings
        settings = {
            'log_type': log_helper.LOG_TYPE_FILE,
            'log_target': config.get(MAIN_SECTION, 'error_log')
            }
        return settings

    def _get_log_settings_foreground(self, config):
        # Log to stdout
        settings = {
            'log_type': log_helper.LOG_TYPE_STREAM,
            'log_target': sys.stdout
            }
        return settings

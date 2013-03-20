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

import logging
from logging.handlers import SysLogHandler
import traceback

_LOG_NAME = 'terasaur'
_LOG_FORMAT_NORMAL = '%(asctime)s %(process)d %(name)s %(levelname)s %(message)s'
_LOG_FORMAT_DEBUG = '%(asctime)s %(process)d %(threadName)s %(name)s %(levelname)s %(message)s'

LOG_TYPE_FILE = 'file'
LOG_TYPE_STREAM = 'stream'
LOG_TYPE_SYSLOG = 'syslog'

_handler = None

class LogHelperException(Exception): pass

""" logging.NullHandler not introduced until python 2.7 """
class NullHandler(logging.Handler):
    def emit(self, record):
        pass

def initialize(type, level, target):
    logger = logging.getLogger(_LOG_NAME)

    # If this is a reinit scenario, need to remove and recreate the
    # handler object
    global _handler
    old_handler = _handler
    if old_handler is not None:
        logger.removeHandler(old_handler)
        _handler = None

    if level == 'debug':
        formatter = logging.Formatter(_LOG_FORMAT_DEBUG)
    else:
        formatter = logging.Formatter(_LOG_FORMAT_NORMAL)

    _handler = _create_handler(type, target)
    _handler.setFormatter(formatter)

    logger.addHandler(_handler)
    logger.setLevel(_translate_level(level))

    if old_handler is not None:
        old_handler.close()

def _translate_level(level):
    if level == 'critical':
        log_level = logging.CRITICAL
    elif level == 'error':
        log_level = logging.ERROR
    elif level == 'warning':
        log_level = logging.WARNING
    elif level == 'info':
        log_level = logging.INFO
    elif level == 'debug':
        log_level = logging.DEBUG
    else:
        raise LogHelperException('Invalid log level: ' + level)
    return log_level

def _create_handler(type, target):
    if type == LOG_TYPE_FILE:
        handler = logging.FileHandler(target)
    elif type == LOG_TYPE_STREAM:
        handler = logging.StreamHandler(target)
    elif type == LOG_TYPE_SYSLOG:
        handler = SysLogHandler()
    else:
        raise LogHelperException('Invalid log target type: ' + type)
    return handler

def get_logger(name=None):
    if name:
        return logging.getLogger(_LOG_NAME + '.' + name)
    else:
        return logging.getLogger(_LOG_NAME)

def print_exception(exc_info):
    (exc_type, exc_value, exc_traceback) = exc_info
    traceback.print_exception(exc_type, exc_value, exc_traceback)

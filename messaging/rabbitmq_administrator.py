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

from rabbitmq_connector import *

import terasaur.log.log_helper as log_helper
import terasaur.config.config_helper as config_helper

class RabbitMQAdministrator(RabbitMQConnector):
    def __init__(self, **kwargs):
        RabbitMQConnector.__init__(self, **kwargs)
        self._exchange_type = 'topic'
        self._exchange_durable = False

    def _run_loop(self):
        """ Called from RabbitMQConnector """
        if self._verbose:
            self._log.info('Calling exchange_declare (exchange: %s)' % (self._exchange))
        self._channel.exchange_declare(exchange=self._exchange, type=self._exchange_type, durable=self._exchange_durable, auto_delete=False)

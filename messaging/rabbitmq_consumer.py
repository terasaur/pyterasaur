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

import time

from rabbitmq_connector import RabbitMQConnector
from rabbitmq_message_handler import PrintingMessageHandler
import terasaur.log.log_helper as log_helper
import terasaur.config.config_helper as config_helper

class RabbitMQConsumer(RabbitMQConnector):
    def __init__(self, **kwargs):
        RabbitMQConnector.__init__(self, **kwargs)

        # consumer only
        self._queue_name = kwargs.get('queue_name', '')
        self._queue_exclusive = kwargs.get('queue_exclusive', False)

        self._handler = kwargs.get('handler', None)
        if not self._handler:
            self._handler = PrintingMessageHandler()

        # callback queue for RPC mode
        #self._callback_queue = ''

    def _run_loop(self):
        """ Called from RabbitMQConnector """
        self._declare_queue()
        self._bind_queue()
        self._consume()

    def _declare_queue(self):
        if self._queue_exclusive:
            if self._verbose:
                self._log.info('Calling queue_declare (exclusive: true)')
            self._queue_name, _, _ = self._channel.queue_declare(durable=False, exclusive=True, auto_delete=False)
        else:
            if self._verbose:
                self._log.info('Calling queue_declare (queue: %s)' % (self._queue_name))
            self._channel.queue_declare(queue=self._queue_name, durable=False, exclusive=False, auto_delete=False)

    def _bind_queue(self):
        routing_key = self._queue_name
        if self._verbose:
            self._log.info('Calling queue_bind(exchange: %s, queue_name: %s, routing_key: %s)' % (self._exchange, self._queue_name, routing_key))
        self._channel.queue_bind(queue=self._queue_name, exchange=self._exchange, routing_key=routing_key)

    def _consume(self):
        while self._running:
            message = self._channel.basic_get(self._queue_name)
            if message is not None:
                self._handle_delivery(message)
            else:
                time.sleep(self._tick_interval)

    def _preflight_check(self):
        RabbitMQConnector._preflight_check(self)
        if not self._queue_exclusive and not self._queue_name:
            raise ValueError('Missing queue name')

    def _handle_delivery(self, message):
        #print 'handle_delivery'
        #print message
        #print message.body

        self._handler.handle(message)
        self._channel.basic_ack(message.delivery_tag)

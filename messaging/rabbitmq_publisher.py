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

import socket
import time
import amqp

from rabbitmq_connector import RabbitMQConnector, RabbitMQConnectorException, DEFAULT_CONTENT_TYPE, DELIVERY_TRANSIENT
import terasaur.log.log_helper as log_helper
import terasaur.config.config_helper as config_helper

class RabbitMQPublisher(RabbitMQConnector):
    def __init__(self, **kwargs):
        RabbitMQConnector.__init__(self, **kwargs)

        # publisher only
        self._content_type = kwargs.get('content_type', DEFAULT_CONTENT_TYPE)
        self._delivery_mode = int(kwargs.get('delivery_mode', DELIVERY_TRANSIENT))
        self._routing_key = kwargs.get('routing_key', None)

    def _run_loop(self):
        """ Called from RabbitMQConnector """
        while self._running:
            # keep the thread from exiting
            time.sleep(self._tick_interval)

    def publish(self, message, **kwargs):
        self._publish(message, **kwargs)

    def _publish(self, message, **kwargs):
        if not self._channel or not self._channel.is_open:
            raise RabbitMQConnectorException('Cannot publish message without open channel')

        # allow specifying a routing_key for alternate destinations
        routing_key = kwargs.get('routing_key', None)
        if not routing_key:
            routing_key = self._routing_key

        if self._verbose:
            self._log.info('Publishing message (exchange: %s, routing key: %s, delivery mode: %i)' %
                           (self._exchange, routing_key, self._delivery_mode))
            #self._log.info('content type: %s' % self._content_type)
            #self._log.info('message: ]%s[' % message)

        amqp_msg = amqp.Message(message)
        amqp_msg.properties['content_type'] = self._content_type
        amqp_msg.properties['delivery_mode'] = self._delivery_mode

        try:
            self._channel.basic_publish(amqp_msg, routing_key=routing_key, exchange=self._exchange)
        except Exception, e:
            self._log.error('Exception publishing message: %s', str(e))

class SelfManagingRabbitMQPublisher(object):
    def __init__(self, **kwargs):
        self._wait_cycles = 0
        self._wait_interval = 0.2 # seconds
        self._wait_timeout = 2 # seconds
        self._timed_out = False
        self._kwargs = kwargs # needed for creating rabbitmq publisher objects
        self._publisher = None
        self._publisher_klass = RabbitMQPublisher
        self._log = log_helper.get_logger(self.__class__.__name__)
        self._verbose = kwargs.get('verbose', False)

    def publish(self, message, **kwargs):
        self._check_or_create_publisher()
        if not self._publisher.connected():
            self._wait_for_connected()
        if self._publisher.connected():
            try:
                self._publisher.publish(message, **kwargs)
            except RabbitMQConnectorException, e:
                self._log.error(str(e))
        else:
            self._log.error('Unable to send message.  MQPublisher not connected.')

    def stop(self):
        if self._publisher:
            self._publisher.stop()
            self._publisher.join()

    def _check_or_create_publisher(self):
        # Need to recycle a publisher that has been disconnected from rabbitmq
        if self._publisher and not self._publisher.is_alive():
            self._publisher = None

        if not self._publisher:
            self._publisher = self._create_publisher(**self._kwargs)
            if self._verbose:
                self._log.info('Calling publisher.start()')
            self._publisher.start()

    def _create_publisher(self, **kwargs):
        if not kwargs.has_key('config'):
            raise RabbitMQConnectorException('Missing config')
        publisher = self._publisher_klass(**kwargs)
        return publisher

    def _wait_for_connected(self):
        """
        Publisher runs in a thread.  Wait for it to complete a rabbitmq connection.
        Implements a timeout, sets self._timed_out to ensure we don't get in an
        endless loop trying to connect.
        """
        if self._verbose:
            self._log.info('Waiting for MQPublisher to connect')
        # We don't want messages to pile up.  Only wait for a
        # connection once.
        if self._timed_out:
            return
        stop = int(self._wait_timeout/self._wait_interval)
        while not self._publisher.connected():
            if self._wait_cycles >= stop:
                self._timed_out = True
                break
            time.sleep(self._wait_interval)
            self._wait_cycles += 1

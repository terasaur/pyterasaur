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

from threading import Thread
import amqp
import terasaur.log.log_helper as log_helper
import terasaur.config.config_helper as config_helper

__all__ = ['RabbitMQConnector', 'RabbitMQConnectorException',
           'DELIVERY_TRANSIENT', 'DELIVERY_PERSISTENT',
           'DEFAULT_CONTENT_TYPE']

DELIVERY_TRANSIENT = 1
DELIVERY_PERSISTENT = 2

CONTENT_TYPE_TEXT = 'text/plain'
CONTENT_TYPE_BINARY = 'application/octet-stream'
DEFAULT_CONTENT_TYPE = CONTENT_TYPE_BINARY

class RabbitMQConnectorException(Exception): pass

class RabbitMQConnector(Thread):
    """
    Abstract class for managing communication with RabbitMQ message
    queues.
    """
    def __init__(self, **kwargs):
        Thread.__init__(self)
        self._config = kwargs.get('config', None)
        self._verbose = bool(kwargs.get('verbose', False))
        self._debug = bool(kwargs.get('debug', False))
        self._log = log_helper.get_logger(self.__class__.__name__)
        self._tick_interval = kwargs.get('tick_interval', 1) # seconds
        self._exchange = None
        self._running = False

        # These are set during the connection process
        self._conn = None
        self._channel = None
        self._ready = False
        self._closed = False

    def run(self):
        """ Called from Thread.start() """
        self._set_params_from_config(self._config)
        self._preflight_check()
        self._connect()
        try:
            self._running = True
            self._run_loop()
        except KeyboardInterrupt:
            self._log.info('Closing RabbitMQ connection')
        self._close_connection()
        self._log.info('RabbitMQ thread terminating')

    def connected(self):
        return self._ready

    def _set_params_from_config(self, config):
        self._exchange = config.get(config_helper.MQ_SECTION, 'exchange')

    def _preflight_check(self):
        if not self._exchange:
            raise ValueError('Missing exchange name')

    def _run_loop(self):
        raise NotImplementedError('Must override abstract method RabbitMQConnector::_run')

    def _connect(self):
        mq_host = self._config.get(config_helper.MQ_SECTION, 'host')
        mq_port = self._config.getint(config_helper.MQ_SECTION, 'port')
        host_port = mq_host+':'+str(mq_port)
        mq_user = self._config.get(config_helper.MQ_SECTION, 'user')
        mq_pass = self._config.get(config_helper.MQ_SECTION, 'pass')
        mq_vhost = self._config.get(config_helper.MQ_SECTION, 'vhost')

        self._log.info('Making RabbitMQ connection to %s (vhost: %s)' % (host_port, mq_vhost))
        self._conn = amqp.Connection(host=host_port, userid=mq_user, password=mq_pass, virtual_host=mq_vhost, insist=False)
        self._channel = self._conn.channel()
        self._set_ready()

    def _set_ready(self):
        if self._verbose:
            self._log.info('Connection is ready')
        self._ready = True

    def stop(self):
        if self._verbose:
            self._log.info('Stopping RabbitMQ thread')
        self._running = False

    def _close_connection(self):
        if self._verbose:
            self._log.info('Calling channel.close()')
        self._channel.close()
        if self._verbose:
            self._log.info('Calling connection.close()')
        self._conn.close()

    def _print_state(self):
        if not self._conn:
            return
        print '************ self._conn.is_open is ' + str(self._conn.is_open)
        print '************ self._conn.is_closing is ' + str(self._conn.is_closing)
        print '************ self._conn.is_closed is ' + str(self._conn.is_closed)

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

import bson
import traceback
import terasaur.log.log_helper as log_helper

class MessageHandler(object):
    def handle(self, message):
        pass

class PrintingMessageHandler(MessageHandler):
    def handle(self, message):
        print 'Basic.Deliver %s delivery-tag %i: %s' % (
                  message.content_type,
                  message.delivery_tag,
                  message.body)

class ControlMessageHandler(MessageHandler):
    def __init__(self, **kwargs):
        """
        Handles control messages received from the message queue.  This is
        treated like a mixin class, which manipulates the internals of a
        SeedbankServer object.
        """
        self._server = kwargs.get('server', None)
        self._verbose = kwargs.get('verbose', False)
        self._log = log_helper.get_logger(self.__class__.__name__)

    def handle(self, message):
        if self._verbose:
            self._log.info('Received control message')

        data = None
        try:
            if message.body:
                decoder = bson.BSON(message.body)
                data = decoder.decode()
            else:
                self._log.error('Empty control message body')

            if data:
                if data.has_key('action'):
                    self._handle_action(data['action'], data)
                else:
                    self._log.warning('Missing action in control message')
        except Exception, e:
            if self._verbose:
                traceback.print_exc()
            self._log.error('Error handling message: ' + str(e))

    def _handle_action(self, action, data):
        raise NotImplementedError('Must override abstract method ControlMessageHandler::_handle_action')

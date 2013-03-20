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
from datetime import datetime, timedelta
import pytz

class TimestampMixin(object):
    def _get_now_timestamp(self):
        # report time in UTC
        now = self._get_now()
        seconds = str(time.mktime(now.timetuple()) - time.timezone)
        ms = '%1.6f' % (now.microsecond * 0.000001)
        ts_str = seconds[:-2] + ms[1:]
        return ts_str

    def _get_now(self):
        return datetime.now(pytz.utc)

    def _seconds_since(self, timestamp):
        if not timestamp:
            raise Exception('Missing timestamp in TimestampMixin::_seconds_since')
        delta = self._get_now() - timestamp
        return delta.days*86400 + delta.seconds

    def _in_the_past(self, now, **kwargs):
        """
        Returns a datetime object in the past.  kwargs follows the timedelta syntax.
        """
        return now - timedelta(**kwargs)

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
import os

def daemonize(stdin='/dev/null', stdout='/dev/null', stderr='/dev/null'):
    """
    Detach and daemonize process.  It's a little different than Stevens' C example,
    more like the Unix Programming FAQ http://www.erlenstar.demon.co.uk/unix/faq_toc.html
    """

    # Fork, divorce from parent, and exit parent process
    try:
        pid = os.fork()
        if pid > 0:
            sys.exit(0)
    except OSError, e:
        sys.stderr.write('First fork error: %d (%s)\n' % (e.errno, e.strerror))
        sys.exit(1)

    # Become session leader
    os.setsid()

    # Fork again, become non-session group leader
    try:
        pid = os.fork()
        if pid > 0:
            sys.exit(0)
    except OSError, e:
        sys.stderr.write('Second fork error: %d (%s)\n' % (e.errno, e.strerror))
        sys.exit(1)

    # Don't leave our pwd in a directory, and set umask
    os.chdir('/')
    os.umask(022)

    # Close fds 0, 1, and 2, and open new descriptors
    os.close(sys.stdin.fileno())
    os.close(sys.stdout.fileno())
    os.close(sys.stderr.fileno())
    # TODO: should probably catch permission errors on opening these files
    sys.stdin = file(stdin, 'r')
    sys.stdout = file(stdout, 'a+')
    sys.stderr = file(stderr, 'a+', 0)

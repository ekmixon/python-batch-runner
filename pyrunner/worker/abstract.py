# Copyright 2019 Comcast Cable Communications Management, LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# SPDX-License-Identifier: Apache-2.0

import traceback, os, sys
import multiprocessing.sharedctypes

import pyrunner.logger.file as lg

from abc import ABC, abstractmethod


class Worker(ABC):
    """
    Abstract class from which user-defined workers must be derived.

    This is responsible for providing the appropriate lifecycle hooks
    that user-defined workers may implement (only run() is mandatory):
        - run()
        - on_success()
        - on_fail()
        - on_exit()
    """

    def __init__(self, context, logfile, argv):
        self.context = context
        self._retcode = multiprocessing.sharedctypes.Value("i", 0)
        self.logfile = logfile
        self.logger = None
        self.argv = argv

    def cleanup(self):
        self._retcode = None

    def _try_exec(self, step, name, rc, required=False):
        try:
            self.retcode = step() or self.retcode
        except NotImplementedError:
            if required:
                raise
        except Exception as e:
            self.logger.error("Uncaught Exception from Worker Thread ({})".format(name))
            self.logger.error(str(e))
            self.logger.error(traceback.format_exc())
            self.retcode = rc

    # The _retcode is handled by multiprocessing.Manager and requires special handling.
    @property
    def retcode(self):
        return self._retcode.value

    @retcode.setter
    def retcode(self, value):
        if int(value) < 0:
            raise ValueError(
                "retcode must be 0 or greater - received: {}".format(value)
            )
        self._retcode.value = int(value)
        return self

    def protected_run(self):
        """
        Initiate worker class run method and additionally trigger other lifecycle
        methods, if defined.
        """
        self.logger = lg.FileLogger(self.logfile).open()
        # Need lower level redirects to capture everything
        os.dup2(self.logger._file_descriptor, 1)
        os.dup2(self.logger._file_descriptor, 2)

        # ON START
        self._try_exec(self.on_start, "ON_START", 902)

        # RUN
        self._try_exec(self.run, "RUN", 903)

        if not self.retcode:
            # ON SUCCESS
            self._try_exec(self.on_start, "ON_SUCCESS", 904)
        else:
            # ON FAIL
            self._try_exec(self.on_fail, "ON_FAIL", 905)

        # ON EXIT
        self._try_exec(self.on_start, "ON_DESTROY", 906)

        self.logger.close()
        self.logger = None

    # To be implemented in user-defined workers.
    def on_start(self):
        """
        Optional lifecycle method. Is only executed when the worker is started/restarted.
        """
        raise NotImplementedError('Method "on_start" is not implemented')

    @abstractmethod
    def run(self):
        """
        Mandatory lifecycle method. The main body of user-defined worker should be
        implemented here.
        """
        pass

    def on_success(self):
        """
        Optional lifecycle method. Is only executed if the run() method returns
        without failure (self.retcode == 0)
        """
        raise NotImplementedError('Method "on_success" is not implemented')

    def on_fail(self):
        """
        Optional lifecycle method. Is only executed if the run() method returns
        without failure (self.retcode != 0)
        """
        raise NotImplementedError('Method "on_fail" is not implemented')

    def on_destroy(self):
        """
        Optional lifecycle method. Is always executed, if implemented, but always
        after on_success() or on_fail().
        """
        raise NotImplementedError('Method "on_destroy" is not implemented')

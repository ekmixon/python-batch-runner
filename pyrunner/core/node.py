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


import pyrunner.logger.file as lg
from pyrunner.worker.abstract import Worker
from pyrunner.core.context import get_context_instance
from pyrunner.core.config import config

import time, multiprocessing, importlib, os


class ExecutionNode:
    """
    The 'mechanical' representation of a Worker. The Node is responsible for
    instantiating the user-defined worker and managing its execution at runtime.

    Each Node maintains a reference to it's parent and child nodes, in addition
    to a variety of runtime statistics/state information.
    """

    def __init__(self, id=-1, name=None):
        if int(id) < -1:
            raise ValueError("id must be -1 or greater")
        if name:
            self.name = name

        self.id = int(id)
        self.argv = []
        self._logfile = None

        # Num attempts/restart management
        self._attempts = 0
        self.max_attempts = 1
        self.retry_wait_time = 0
        self._wait_until = 0

        self._start_time = 0
        self._end_time = 0
        self.timeout = float("inf")
        self._proc = None
        self.context = get_context_instance()

        # Service execution mode properties
        self.exec_interval = 1

        self.module = None
        self.worker = None
        self._worker_instance = None

        self.parent_nodes = set()
        self.child_nodes = set()

    def __hash__(self):
        return hash(self.id)

    def __eq__(self, other):
        return self.id == other.id

    def __ne__(self, other):
        return not (self.id == other.id)

    def __lt__(self, other):
        return self.id < other.id

    def is_runnable(self):
        return time.time() >= self._wait_until

    def revive(self):
        self._attempts = 0
        self._wait_until = time.time() + self.exec_interval

    @property
    def logfile(self):
        return self._logfile
    
    @logfile.setter
    def logfile(self, logfile):
        expanded = os.path.expandvars(logfile)
        if not os.path.isabs(expanded):
            self._logfile = os.path.join(config.getstring("framework", "log_dir"), expanded)
        else:
            self._logfile = expanded

    def execute(self):
        """
        Spawns a new process via the `run` method of defined Worker class.

        Utilizes multiprocessing's Process to fork a new process to execute the `run` method implemented
        in the provided Worker class.

        Workers are given references to the shared Context, main-proc <-> child-proc return code value,
        logfile handle, and task-level arguments.
        """
        # Return early if retry triggered and wait time has not yet fully elapsed
        if not self.is_runnable():
            return

        self._attempts += 1

        if not self._start_time:
            self._start_time = time.time()

        try:
            # Check if provided worker actually extends the Worker class.
            if not issubclass(self.worker_class, Worker):
                raise TypeError(
                    "{}.{} is not an extension of pyrunner.Worker".format(
                        self.module, self.worker
                    )
                )

            # Launch the "run" method of the provided Worker under a new process.
            self._worker_instance = self.worker_class(
                self.context, self.logfile, self.argv
            )
            self._proc = multiprocessing.Process(
                target=self._worker_instance.protected_run, daemon=False
            )
            self._proc.start()
        except Exception as e:
            import traceback

            print(traceback.format_exc())
            logger = lg.FileLogger(self.logfile)
            logger.open()
            logger.error(str(e))
            logger.close()

    def poll(self, wait=False):
        """
        Polls the running process for completion and returns the worker's return code. None if still running.

        Args:
          wait (bool): If enabled (set to True), the `poll` method will be a blocking call.
                       If False (default behavior), the method will not wait until the completion
                       of the child process and return `None`, if proc is still running.

        Returns:
          Integer return code if process has exited, otherwise `None`.
        """
        if not self._proc:
            return 901

        running = self._proc.is_alive()
        retcode = 0

        if not running or wait:
            # Note that if wait is True, then the join() method is invoked immediately,
            # causing the thread to block until it's job is complete.
            self._proc.join()
            self._end_time = time.time()
            retcode = self._worker_instance.retcode
            if retcode > 0 and (self._attempts < self.max_attempts):
                logger = lg.FileLogger(self.logfile)
                logger.open(False)
                self._wait_until = time.time() + self.retry_wait_time
                logger.restart_message(
                    self._attempts,
                    "Waiting {} seconds before retrying...".format(
                        self.retry_wait_time
                    ),
                )
                logger.close(False)
                retcode = -1
            self.cleanup()
        elif (time.time() - self._start_time) >= self.timeout:
            retcode = self.terminate(
                "Worker runtime has exceeded the set maximum/timeout of {} seconds.".format(
                    self.timeout
                )
            )
            running = False

        return retcode if (not running or wait) else None

    def terminate(self, message="Terminating process"):
        """
        Immediately terminates the Worker, if running.
        """
        if self._proc.is_alive():
            self._proc.terminate()
            logger = lg.FileLogger(self.logfile)
            logger.open(False)
            logger._system_(message)
            logger.close()
        self.cleanup()
        return 907

    def cleanup(self):
        self._proc = None
        self.context = None
        self._worker_instance = None

    # ########################## MISC ########################## #

    def get_node_by_id(self, id):
        if self.id == id:
            return self
        elif not self.child_nodes:
            return None
        else:
            for n in self.child_nodes:
                temp = n.get_node_by_id(id)
                if temp:
                    return temp
        return None

    def get_node_by_name(self, name):
        if self.name == name:
            return self
        elif not self.child_nodes:
            return None
        else:
            for n in self.child_nodes:
                temp = n.get_node_by_name(name)
                if temp:
                    return temp
        return None

    def add_parent_node(self, parent):
        self.parent_nodes.add(parent)

    def add_child_node(self, child, parent_id_list, named_deps=False):
        if (named_deps and self.name in [x for x in parent_id_list]) or (
            not named_deps and self.id in [int(x) for x in parent_id_list]
        ):
            child.add_parent_node(self)
            self.child_nodes.add(child)
        for c in self.child_nodes:
            c.add_child_node(child, parent_id_list, named_deps)

    def pretty_print(self, indent=""):
        print("{}{} - {}".format(indent, self.id, self.name))
        for c in self.child_nodes:
            c.pretty_print("{}  ".format(indent))

    def get_elapsed_time(self):
        end_time = self._end_time if self._end_time else time.time()

        if self._start_time and end_time and end_time > self._start_time:
            elapsed_time = end_time - self._start_time
            return time.strftime("%H:%M:%S", time.gmtime(elapsed_time))
        else:
            return "00:00:00"

    @property
    def worker_class(self):
        return getattr(importlib.import_module(self.module), self.worker)
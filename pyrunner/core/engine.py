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

from pyrunner.core import config
from pyrunner.core.context import Context
from pyrunner.core.signal import SignalHandler, SIG_ABORT, SIG_PULSE, SIG_REVIVE
from multiprocessing import Manager

import sys, time, os


class ExecutionEngine:
    """
    The heart of all worker execution. Delegates state management of each
    ExecutionNode to the NodeRegister and triggers each node in the appropriate
    order.
    """

    def __init__(self):
        self.register = None
        self.start_time = None
        self._wait_until = 0

        # Initialization of Manager proxy objects and Context
        self._manager = Manager()
        self._shared_dict = self._manager.dict()
        self._shared_queue = self._manager.Queue()
        self.context = Context(self._shared_dict, self._shared_queue)

        # Lifecycle hooks
        self._on_create_func = None
        self._on_start_func = None
        self._on_restart_func = None
        self._on_success_func = None
        self._on_fail_func = None
        self._on_destroy_func = None

    def on_create(self, func):
        self._on_create_func = func

    def on_start(self, func):
        self._on_start_func = func

    def on_restart(self, func):
        self._on_restart_func = func

    def on_success(self, func):
        self._on_success_func = func

    def on_fail(self, func):
        self._on_fail_func = func

    def on_destroy(self, func):
        self._on_destroy_func = func

    def initiate(self, **kwargs):
        """Begins the execution loop."""

        signal_handler = SignalHandler()
        sys.path.append(config["worker_dir"])
        self.start_time = time.time()
        wait_interval = 1.0 / config["tickrate"] if config["tickrate"] >= 1 else 0
        last_save = 0

        if not self.register:
            raise RuntimeError("NodeRegister has not been initialized!")

        # App lifecycle - RESTART
        if config["restart"]:
            if self._on_restart_func:
                self._on_restart_func()
        # App lifecycle - CREATE
        else:
            if self._on_create_func:
                self._on_create_func()

        # App lifecycle - START
        if self._on_start_func:
            self._on_start_func()

        # Execution loop
        try:
            while self.register.running_nodes or self.register.pending_nodes:
                # Consume pulse signal, if any, to indicate app is already running
                signal_handler.consume(SIG_PULSE)

                # Check for abort signals
                if signal_handler.consume(SIG_ABORT):
                    print("ABORT signal received! Terminating all running Workers.")
                    self._abort_all_workers()
                    return -1

                # Check for revive signals; revive failed nodes, if any
                if signal_handler.consume(SIG_REVIVE):
                    for node in self.register.failed_nodes.copy():
                        node.revive()
                        self.register.failed_nodes.remove(node)
                        self.register.pending_nodes.add(node)
                    for node in self.register.defaulted_nodes.copy():
                        self.register.defaulted_nodes.remove(node)
                        self.register.pending_nodes.add(node)

                # Poll running nodes for completion/failure
                for node in self.register.running_nodes.copy():
                    retcode = node.poll()
                    if retcode is not None:
                        self.register.running_nodes.remove(node)
                        if retcode > 0:
                            self.register.failed_nodes.add(node)
                            self.register.set_children_defaulted(node)
                        elif retcode < 0:
                            self.register.pending_nodes.add(node)
                        else:
                            self.register.completed_nodes.add(node)

                # Check pending nodes for eligibility to execute
                for node in self.register.pending_nodes.copy():
                    if (
                        config["max_procs"] > 0
                        and len(self.register.running_nodes) >= config["max_procs"]
                    ):
                        break

                    if not time.time() >= self._wait_until:
                        break

                    self._wait_until = time.time() + config["time_between_tasks"]
                    runnable = True
                    for p in node.parent_nodes:
                        if p.id >= 0 and p not in self.register.completed_nodes.union(
                            self.register.norun_nodes
                        ):
                            runnable = False
                            break
                    if runnable and node.is_runnable():
                        self.register.pending_nodes.remove(node)
                        node.context = self.context
                        node.execute()
                        self.register.running_nodes.add(node)

                if not kwargs.get("silent") and not config["silent"]:
                    self._print_current_state()

                # Check for input requests from interactive mode
                while (
                    self.context
                    and self.context.shared_queue
                    and not self.context.shared_queue.empty()
                ):
                    key = self.context.shared_queue.get()
                    value = input("Please provide value for '{}': ".format(key))
                    self.context.set(key, value)

                # Persist state to disk at set intervals
                if (
                    not config["test_mode"]
                    and self.save_state
                    and (time.time() - last_save) >= config["save_interval"]
                ):
                    self.save_state(True)
                    last_save = time.time()

                # Wait
                if wait_interval > 0:
                    time.sleep(
                        wait_interval
                        - ((time.time() - self.start_time) % wait_interval)
                    )
        except KeyboardInterrupt:
            print("\nKeyboard Interrupt Received")
            print("\nCancelling Execution")
            self._abort_all_workers()
            return -1

        # App lifecycle - SUCCESS
        if len(self.register.failed_nodes) == 0:
            if self._on_success_func:
                self._on_success_func()
        # App lifecycle - FAIL (<0 is for ABORT or other interrupt)
        elif len(self.register.failed_nodes) > 0 and self._on_fail_func:
            self._on_fail_func()

        # App lifecycle - DESTROY
        if self._on_destroy_func:
            self._on_destroy_func()

        if config["dump_logs"] or (not kwargs.get("silent") and not config["silent"]):
            self._print_final_state()

        if not config["test_mode"] and self.save_state:
            self.save_state()

        return len(self.register.failed_nodes)

    def _abort_all_workers(self):
        for node in self.register.running_nodes.copy():
            node.terminate(
                "Keyboard Interrupt (SIGINT) received. Terminating Worker and exiting."
            )
            self.register.running_nodes.remove(node)
            self.register.aborted_nodes.add(node)
            self.register.set_children_defaulted(node)
        self.save_state(False, True)
        self._print_final_state(True)

    def _print_current_state(self):
        elapsed = time.time() - self.start_time

        if not config["debug"]:
            print(
                "Pending: {} | Running: {} | Completed: {} | Failed: {} | Defaulted: {} | Time Elapsed: {:0.2f} sec.".format(
                    len(self.register.pending_nodes),
                    len(self.register.running_nodes),
                    len(self.register.completed_nodes),
                    len(self.register.failed_nodes),
                    len(self.register.defaulted_nodes),
                    elapsed,
                ),
                flush=True,
            )
        else:
            print(chr(27) + "[2J")
            print("Elapsed Time: {:0.2f}".format(elapsed))
            if self.register.pending_nodes:
                print("\nPENDING TASKS")
            for p in self.register.pending_nodes:
                print("  {} - {}".format(p.id, p.name))
            if self.register.failed_nodes.union(self.register.defaulted_nodes):
                print("\nFAILED TASKS")
            for p in self.register.failed_nodes.union(self.register.defaulted_nodes):
                print("  {} - {}".format(p.id, p.name))
            if self.register.running_nodes:
                print("\nRUNNING TASKS")
            for p in self.register.running_nodes:
                print("  {} - {}".format(p.id, p.name))

    def _print_final_state(self, aborted=False):
        print("\nCompleted in {:0.2f} seconds\n".format(time.time() - self.start_time))

        if aborted:
            print("Final Status: ABORTED\n")
            print("Aborted Processes:\n")

            for n in self.register.aborted_nodes:
                self._print_node_info(n, config["dump_logs"])

        elif len(self.register.failed_nodes) + len(self.register.defaulted_nodes):
            print("Final Status: FAILURE\n")
            print("Failed Processes:\n")

            for n in self.register.failed_nodes:
                self._print_node_info(n, config["dump_logs"])

        else:
            print("Final Status: SUCCESS\n")

    def _print_node_info(self, n, dump_logs=False):
        if dump_logs:
            print(
                "############################################################################"
            )

        print("# ID: {}".format(n.id))
        print("# Name: {}".format(n.name))
        print("# Module: {}".format(n.module))
        print("# Worker: {}".format(n.worker))
        print("# Arguments: {}".format(n.arguments))
        print("# Log File: {}".format(n.logfile))

        if dump_logs:
            with open(n.logfile, "r") as f:
                for line in f:
                    print(line, end="")

        print("")

    def save_state(self, suppress_output=False, only_ctllog=False):
        if not suppress_output:
            print("Saving Execution Graph File to: {}".format(config.ctllog_file))

        self.serde_obj.save_to_file(config.ctllog_file, self.register)
        if only_ctllog:
            return

        try:

            state_obj = {
                "config": config.items(),
                "shared_dict": self.engine._shared_dict.copy(),
            }

            if not suppress_output:
                print("Saving Context Object to File: {}".format(config.ctx_file))
            tmp = config.ctx_file + ".tmp"
            perm = config.ctx_file
            pickle.dump(state_obj, open(tmp, "wb"))
            if os.path.isfile(perm):
                os.unlink(perm)
            os.rename(tmp, perm)

        except Exception:
            print("Failure in save_context()")
            raise
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

import os, sys
import glob, shutil, zipfile
import getopt

import pyrunner.jobspec as jobspec
import pyrunner.notification as notification
#import pyrunner.autodoc.introspection as intro
import pyrunner.core.constants as constants

from pyrunner.core.config import config
from pyrunner.core.context import create_new_context
from pyrunner.core.register import NodeRegister
from pyrunner.core.signal import SignalHandler, SIG_ABORT, SIG_REVIVE, SIG_PULSE
from pyrunner.version import __version__

from datetime import datetime as datetime
import pickle
import time


class PyRunner:
    def __init__(self, **kwargs):
        self.context = create_new_context()
        self.notification = notification.EmailNotification()
        self.signal_handler = SignalHandler()
        self.register = NodeRegister()
        self.jobspec = jobspec.ListFileJobSpec()
        self.sig_handler = SignalHandler()
        self.start_time = None
        self.restart = False
        self._wait_until = 0

        # Lifecycle hooks
        self._on_create_func = lambda *args: None
        self._on_start_func = lambda *args: None
        self._on_restart_func = lambda *args: None
        self._on_success_func = lambda *args: None
        self._on_fail_func = lambda *args: None
        self._on_destroy_func = lambda *args: None

        # Signal switches
        self._revive = False
        self._abort = False

        # Parse cmd args
        self.parse_args(kwargs.get("parse_args", True))

        if "config_file" in kwargs:
            config.load_cfg(kwargs["config_file"])

        ctllog_file = f"{config.get('framework', 'temp_dir')}/{config.get('framework', 'app_name')}.ctllog"
        ctx_file = f"{config.get('framework', 'temp_dir')}/{config.get('framework', 'app_name')}.ctx"
        if self.restart and os.path.isfile(ctllog_file):
            self.jobspec.load(ctllog_file)
            self.context.load_from_file(ctx_file)
        elif "job_spec" in kwargs:
            self.restart = False # In case we're starting new run despite -r flag
            self.register = self.jobspec.load(kwargs["job_spec"])

    def dup_proc_is_running(self):
        self.signal_handler.emit(SIG_PULSE)
        time.sleep(1.1)
        if SIG_PULSE not in self.signal_handler.peek():
            print(self.signal_handler.peek())
            return True
        else:
            return False

    @property
    def version(self):
        return __version__

    #@property
    #def notification(self):
    #    return self._notification

    #@notification.setter
    #def notification(self, o):
    #    if not issubclass(type(o), Notification):
    #        raise TypeError("Not an extension of pyrunner.notification.Notification")
    #    self._notification = o
    #    return self

    #def plugin_serde(self, obj):
    #    if not isinstance(obj, serde.SerDe):
    #        raise TypeError("SerDe plugin must implement the SerDe interface")
    #    self.serde_obj = obj
#
    #def plugin_notification(self, obj):
    #    if not isinstance(obj, notification.Notification):
    #        raise TypeError(
    #            "Notification plugin must implement the Notification interface"
    #        )
    #    self.notification = obj

    # Job lifecycle hooks decorators
    def on_create(self, func):
        self._on_create_func(func)
    def on_start(self, func):
        self._on_start_func(func)
    def on_restart(self, func):
        self._on_restart_func(func)
    def on_success(self, func):
        self._on_success_func(func)
    def on_fail(self, func):
        self._on_fail_func(func)
    def on_destroy(self, func):
        self._on_destroy_func(func)

    # NodeRegister wiring
    def add_node(self, **kwargs):
        return self.register.add_node(**kwargs)
    def exec_only(self, id_list):
        return self.register.exec_only(id_list)
    def exec_to(self, id):
        return self.register.exec_to(id)
    def exec_from(self, id):
        return self.register.exec_from(id)
    def exec_disable(self, id_list):
        return self.register.exec_disable(id_list)

    def process_signals(self):
        if self._abort:
            print(
                f"Submitting ABORT signal to running job for: {config.get('framework', 'app_name')}"
            )
            self.signal_handler.emit(SIG_ABORT)
            sys.exit(0)

        if self._revive:
            print(
                f"Submitting REVIVE signal to running job for: {config.get('framework', 'app_name')}"
            )
            self.signal_handler.emit(SIG_REVIVE)
            sys.exit(0)
        
        if self.dup_proc_is_running():
            raise OSError(
                f'Another process for "{config.get("framework", "app_name")}" is already running!'
            )
        
        # Clear signals, if any, to ensure clean start.
        self.signal_handler.consume_all()


    def prepare(self):
        # Modify NodeRegister
        """
        if config["exec_proc_name"]:
            self.exec_only([self.register.find_node(name=config["exec_proc_name"]).id])
        if config["exec_only_list"]:
            self.exec_only(config["exec_only_list"])
        if config["exec_disable_list"]:
            self.exec_disable(config["exec_disable_list"])
        if config["exec_from_id"] is not None:
            self.exec_from(config["exec_from_id"])
        if config["exec_to_id"] is not None:
            self.exec_to(config["exec_to_id"])
        """

    def execute(self, **kwargs):
        """Begins the execution loop."""
        print(f"Executing PyRunner App: {config.get('framework', 'app_name')}")
        sys.path.append(config.get("framework", "worker_dir"))

        os.makedirs(config.get("framework", "temp_dir"), exist_ok=True)
        os.makedirs(config.get("framework", "log_dir"), exist_ok=True)

        self.process_signals()
        self.start_time = time.time()
        wait_interval = (
            1.0 / config.getint("launch_params", "tickrate")
            if config.getint("launch_params", "tickrate") >= 1
            else 0
        )
        last_save = 0

        # App lifecycle - RESTART or CREATE
        self._on_restart_func() if self.restart else self._on_create_func()

        # App lifecycle - START
        self._on_start_func()

        # Execution loop
        try:
            while self.register.running_nodes or self.register.pending_nodes:
                # Consume pulse signal, if any, to indicate app is already running
                self.sig_handler.consume(SIG_PULSE)

                # Check for abort signals
                if self.sig_handler.consume(SIG_ABORT):
                    print("ABORT signal received! Terminating all running Workers.")
                    self._abort_all_workers()
                    return -1

                # Check for revive signals; revive failed nodes, if any
                if self.sig_handler.consume(SIG_REVIVE):
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
                        config.getint("launch_params", "max_procs") > 0
                        and len(self.register.running_nodes) >= config.getint("launch_params", "max_procs")
                    ):
                        break

                    if not time.time() >= self._wait_until:
                        break

                    self._wait_until = time.time() + config.getint("launch_params", "time_between_tasks")
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

                if not kwargs.get("silent") and not config.getboolean("launch_params", "silent"):
                    self._print_current_state()

                # Check for input requests from interactive mode
                while (not self.context.shared_queue.empty()):
                    key = self.context.shared_queue.get()
                    value = input("Please provide value for '{}': ".format(key))
                    self.context.set(key, value)

                # Persist state to disk at set intervals
                if (
                    not config.getboolean("launch_params", "test_mode")
                    and self.save_state
                    and (time.time() - last_save) >= config.getint("launch_params", "save_interval")
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
            self._on_success_func()
        # App lifecycle - FAIL (<0 is for ABORT or other interrupt)
        elif len(self.register.failed_nodes) > 0:
            self._on_fail_func()

        # App lifecycle - DESTROY
        self._on_destroy_func()

        if config.getboolean("launch_params", "dump_logs") or (
            not kwargs.get("silent")
            and not config.getboolean("launch_params", "silent")
        ):
            self._print_final_state()

        if (
            not config.getboolean("launch_params", "test_mode")
            and self.save_state
        ):
            self.save_state()

        retcode = len(self.register.failed_nodes)

        should_notify = True
        if retcode == 0 and not config["launch_params"]["notify_on_success"]:
            print(
                'Skipping Notification: Property "notify_on_success" is set to FALSE.'
            )
            should_notify = False
        elif retcode > 0 and not config["launch_params"]["notify_on_fail"]:
            print('Skipping Notification: Property "notify_on_fail" is set to FALSE.')
            should_notify = False

        if should_notify:
            self.notification.emit_notification(config, self.register)

        if not config["launch_params"]["nozip"]:
            self.zip_log_files(retcode)

        self.cleanup_log_files()

        if retcode == 0:
            self.delete_state()

        return retcode

    def _abort_all_workers(self):
        for node in self.register.running_nodes.copy():
            node.terminate(
                "Keyboard Interrupt (SIGINT) received. Terminating Worker and exiting."
            )
            self.register.running_nodes.remove(node)
            self.register.aborted_nodes.add(node)
            self.register.set_children_defaulted(node)
        self.save_state(False)
        self._print_final_state(True)

    def _print_current_state(self):
        elapsed = time.time() - self.start_time

        if not config.getboolean("launch_params", "debug"):
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
                self._print_node_info(
                    n, config.getboolean("launch_params", "dump_logs")
                )

        elif len(self.register.failed_nodes) + len(self.register.defaulted_nodes):
            print("Final Status: FAILURE\n")
            print("Failed Processes:\n")

            for n in self.register.failed_nodes:
                self._print_node_info(
                    n, config.getboolean("launch_params", "dump_logs")
                )

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
        print("# Arguments: {}".format(n.argv))
        print("# Log File: {}".format(n.logfile))

        if dump_logs:
            with open(n.logfile, "r") as f:
                for line in f:
                    print(line, end="")

        print("")

    def save_state(self, suppress_output=False):
        if not config.get("framework", "temp_dir"):
            return

        ctllog_file = "{}/{}.ctllog".format(
            config.get("framework", "temp_dir"),
            config.get("framework", "app_name")
        )
        if not suppress_output:
            print("Saving Execution Graph File to: {}".format(ctllog_file))
        self.jobspec.dump(ctllog_file, self.register)

        ctx_file = "{}/{}.ctx".format(
            config.get("framework", "temp_dir"),
            config.get("framework", "app_name")
        )
        if not suppress_output:
            print("Saving Context Object to File: {}".format(ctx_file))
        self.context.save_to_file(ctx_file)

    def cleanup_log_files(self):
        if config.getint("framework", "log_retention") < 0:
            return

        try:
            files = glob.glob("{}/*".format(config.get("framework", "log_root_dir")))
            to_delete = [
                f
                for f in files
                if os.stat(f).st_mtime
                < (time.time() - (config.getint("framework", "log_retention") * 86400.0))
            ]

            if to_delete:
                print("Cleaning Up Old Log Files")

            for f in to_delete:
                print("Deleting File/Directory: {}".format(f))
                if os.path.isdir(f):
                    shutil.rmtree(f)
                else:
                    os.remove(f)
        except Exception:
            print("Failure in cleanup_log_files()")
            raise

    def zip_log_files(self, exit_status):
        node_list = list(self.register.all_nodes)
        zf = None
        zip_file = None

        try:

            if exit_status == -1:
                suffix = "ABORT"
            elif exit_status > 0:
                suffix = "FAILURE"
            else:
                suffix = "SUCCESS"

            zip_file = "{}/{}_{}_{}.zip".format(
                config.get("framework", "log_dir"),
                config.get("framework", "app_name"),
                constants.EXECUTION_TIMESTAMP,
                suffix,
            )
            print("Zipping Up Log Files to: {}".format(zip_file))
            zf = zipfile.ZipFile(zip_file, "w", zipfile.ZIP_DEFLATED)

            for node in node_list:
                if node.id != -1 and node not in self.register.pending_nodes.union(
                    self.register.defaulted_nodes
                ):
                    logfile = node.logfile
                    if os.path.isfile(logfile):
                        zf.write(logfile, os.path.basename(logfile))
                        os.remove(logfile)

            zf.write(config.ctllog_file, os.path.basename(config.ctllog_file))

        except Exception:
            print("Failure in zip_log_files()")
            raise
        finally:
            if zf:
                zf.close()

        return zip_file

    def load_state(self):
        if not self.load_proc_file(config.ctllog_file, True):
            return False

        if not os.path.isfile(config.ctx_file):
            return False

        print("Loading prior Context from {}".format(config.ctx_file))
        state_obj = pickle.load(open(config.ctx_file, "rb"))

        for k, v in state_obj["config"].items():
            config[k] = v

        for k, v in state_obj["shared_dict"].items():
            self.engine._shared_dict[k] = v

        return True

    def delete_state(self):
        if os.path.isfile(config.ctllog_file):
            os.remove(config.ctllog_file)
        if os.path.isfile(config.ctx_file):
            os.remove(config.ctx_file)

    def is_restartable(self):
        if not os.path.isfile(config.ctllog_file):
            return False
        if not os.path.isfile(config.ctx_file):
            return False
        return True

    def print_documentation(self):
        while self.register.pending_nodes:
            for node in self.register.pending_nodes.copy():
                runnable = True
                for p in node.parent_nodes:
                    if p.id >= 0 and p not in self.register.completed_nodes.union(
                        self.register.norun_nodes
                    ):
                        runnable = False
                        break
                if runnable:
                    self.register.pending_nodes.remove(node)
                    intro.print_context_usage(node)
                    self.register.completed_nodes.add(node)

    def parse_args(self, run_getopts=True):
        opt_list = "n:e:x:N:D:A:t:drhiv"
        longopt_list = [
            "setup", "help", "nozip", "interactive", "abort",
            "restart", "version", "debug", "silent", "dump-logs",
            "allow-duplicate-jobs", "email=", "email-on-fail=",
            "email-on-success=", "env=", "cvar=", "context=",
            "time-between-tasks=", "to=", "from=", "descendants=",
            "ancestors=", "norun=", "exec-only=", "exec-proc-name=",
            "max-procs=", "notify-on-fail=", "notify-on-success=",
            "revive"
        ]

        if run_getopts:
            try:
                opts, _ = getopt.getopt(sys.argv[1:], opt_list, longopt_list)
            except getopt.GetoptError as e:
                print(str(e))
                self.show_help()
                sys.exit(1)

            for opt, arg in opts:
                if opt in ["-d", "--debug"]:
                    config['launch_params']["debug"] = True
                elif opt in ["-n", "--max-procs"]:
                    config['launch_params']["max_procs"] = int(arg)
                elif opt in ["-r", "--restart"]:
                    self.restart = True
                elif opt in ["-x", "--exec-only"]:
                    config['launch_params']["exec_only_list"] = [int(id) for id in arg.split(",")]
                elif opt in ["-N", "--norun"]:
                    config['launch_params']["exec_disable_list"] = [int(id) for id in arg.split(",")]
                elif opt in ["-D", "--from", "--descendents"]:
                    config['launch_params']["exec_from_id"] = int(arg)
                elif opt in ["-A", "--to", "--ancestors"]:
                    config['launch_params']["exec_to_id"] = int(arg)
                elif opt in ["-e", "--email"]:
                    config['launch_params']["email"] = arg
                elif opt == "--notify-on-fail":
                    config['launch_params']["notify_on_fail"] = arg
                elif opt == "--notify-on-success":
                    config['launch_params']["notify_on_success"] = arg
                elif opt == "--env":
                    parts = arg.split("=")
                    os.environ[parts[0]] = parts[1]
                elif opt == "--cvar":
                    parts = arg.split("=")
                    self.context[parts[0]] = parts[1]
                elif opt == "--nozip":
                    config["nozip"] = True
                elif opt == "--dump-logs":
                    config["dump_logs"] = True
                elif opt in ["-i", "--interactive"]:
                    self.context.interactive = True
                elif opt in ["-t", "--tickrate"]:
                    config["tickrate"] = int(arg)
                elif opt in ["--time-between-tasks"]:
                    config["time_between_tasks"] = int(arg)
                elif opt in ["--allow-duplicate-jobs"]:
                    config["allow_duplicate_jobs"] = True
                elif opt in ["--exec-proc-name"]:
                    config["exec_proc_name"] = arg
                elif opt == "--revive":
                    self._revive = True
                elif opt == "--abort":
                    self._abort = True
                elif opt == "--silent":
                    config["silent"] = True
                elif opt in ("-h", "--help"):
                    self.show_help()
                    sys.exit(0)
                elif opt in ("-v", "--version"):
                    print("PyRunner v{}".format(__version__))
                    sys.exit(0)
                else:
                    raise ValueError("Error during parsing of opts")

    def show_help(self):
        print(
            """
            Options:
                -c <path>                                 Path to config file.
                -l <path>                                 Path to process list filename.
                -r,  --restart                            Start from last known point-of-failure, if any.
                -n,  --max_procs <num>                    Maximum number of concurrent processes.
                     --exec-proc-name <proc name>         Execute only a single process/task with the given name.
                -x,  --exec-only <comma seperated nums>   Comma separated list of process ID's to execute. All other processes will be set to NORUN.
                -N,  --norun <comma separated nums>       Comma separated list of process ID's to NOT execute (set to NORUN).
                -D,  --descendents <comma separated nums> Comma separated list of process ID's to execute, along with their descendent processes (child procs and beyond).
                -A,  --ancestors <comma separated nums>   Comma separated list of process ID's to execute, along with their ancestors processes (parent procs and beyond).
                -e,  --email <email>                      Email to send job notification email upon completion or failure.
                     --email-on-fail <true|false>         Enable/disable job notification email upon failure.
                     --email-on-success <true|false>      Enable/disable job notification email upon success.
                -d,  --debug                              Prints list of Pending, Running, Failed, and Defaulted tasks instead of summary counts.
                -i,  --interactive                        Interactive mode. This will force the execution engine to request user input for each non-existent Context variable.
                     --env <VAR_NAME=var_value>           Provide key/value pair to export to the environment prior to execution. Can provide this option multiple times.
                     --ctx <VAR_NAME=var_value>           Provide key/value pair to initialize the Context object with prior to execution. Can provide this option multiple times.
                     --nozip                              Disable behavior which zips up all log files after job exit.
                     --dump-logs                          Enable behavior which prints all failure logs, if any, to STDOUT after job exit.
                -t,  --tickrate <num>                     Number of times per second that the executon engine should poll child processes/launch new processes. Default is 1.
                     --time-between-tasks <seconds>       Number of seconds, at minimum, that the execution engine should wait after launching a process before launching another.
                     --allow-duplicate-jobs               Enables running more than 1 instance of a unique job (based on APP_NAME).
                     --abort                              Aborts running instance of a job (based on APP_NAME), if any.
                     --setup                              Run the PyRunner basic project setup.
                -v,  --version                            Print PyRunner version.
                -h,  --help                               Show help (you're reading it right now).
            """
        )

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
import glob
import shutil
import zipfile
import getopt

import pyrunner.serde as serde
import pyrunner.notification as notification
import pyrunner.autodoc.introspection as intro
import pyrunner.core.constants as constants

from pyrunner.core import config
from pyrunner.core.engine import ExecutionEngine
from pyrunner.core.register import NodeRegister
from pyrunner.core.signal import SignalHandler, SIG_ABORT, SIG_REVIVE, SIG_PULSE
from pyrunner.version import __version__

from pyrunner.notification import Notification

from datetime import datetime as datetime
import pickle
import time


class PyRunner:
    def __init__(self, **kwargs):
        self._environ = os.environ.copy()
        self._notification = notification.EmailNotification()
        self.signal_handler = SignalHandler()

        self.serde_obj = serde.ListSerDe()
        self.register = NodeRegister()
        self.engine = ExecutionEngine()

        config["config_file"] = kwargs.get("config_file")
        config["proc_file"] = kwargs.get("proc_file")
        config["restart"] = kwargs.get("restart", False)

        self.parse_args(kwargs.get("parse_args", True))

        if self.dup_proc_is_running():
            raise OSError(
                'Another process for "{}" is already running!'.format(
                    config["app_name"]
                )
            )
        else:
            # Clear signals, if any, to ensure clean start.
            self.signal_handler.consume_all()

    def reset_env(self):
        os.environ.clear()
        os.environ.update(self._environ)

    def dup_proc_is_running(self):
        self.signal_handler.emit(SIG_PULSE)
        time.sleep(1.1)
        if SIG_PULSE not in self.signal_handler.peek():
            print(self.signal_handler.peek())
            return True
        else:
            return False

    def load_proc_file(self, proc_file, restart=False):
        if not proc_file or not os.path.isfile(proc_file):
            return False

        self.register = self.serde_obj.deserialize(proc_file, restart)

        if not self.register or not isinstance(self.register, NodeRegister):
            return False

        return True

    @property
    def notification(self):
        return self._notification

    @notification.setter
    def notification(self, o):
        if not issubclass(type(o), Notification):
            raise TypeError("Not an extension of pyrunner.notification.Notification")
        self._notification = o
        return self

    @property
    def version(self):
        return __version__

    @property
    def log_dir(self):
        return config["log_dir"]

    @property
    def config_file(self):
        return config["config_file"]

    @config_file.setter
    def config_file(self, value):
        config["config_file"] = value
        return self

    @property
    def proc_file(self):
        return config["proc_file"]

    @proc_file.setter
    def proc_file(self, value):
        config["proc_file"] = value
        return self

    @property
    def context(self):
        return self.engine.context

    @property
    def restart(self):
        return config["restart"]

    def plugin_serde(self, obj):
        if not isinstance(obj, serde.SerDe):
            raise TypeError("SerDe plugin must implement the SerDe interface")
        self.serde_obj = obj

    def plugin_notification(self, obj):
        if not isinstance(obj, notification.Notification):
            raise TypeError(
                "Notification plugin must implement the Notification interface"
            )
        self.notification = obj

    # Engine wiring
    def on_create(self, func):
        self.engine.on_create(func)

    def on_start(self, func):
        self.engine.on_start(func)

    def on_restart(self, func):
        self.engine.on_restart(func)

    def on_success(self, func):
        self.engine.on_success(func)

    def on_fail(self, func):
        self.engine.on_fail(func)

    def on_destroy(self, func):
        self.engine.on_destroy(func)

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

    def prepare(self):
        # Initialize NodeRegister
        if config["restart"]:
            self.load_state()
        elif config["proc_file"]:
            self.load_proc_file(config["proc_file"])

        # Inject Context var overrides
        for k, v in config["cvar_list"]:
            self.engine.context.set(k, v)

        # Modify NodeRegister
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

    def execute(self):
        return self.run()

    def run(self):
        self.prepare()

        # Prepare engine
        self.engine.register = self.register

        # Short circuit for a dryrun
        if config["dryrun"]:
            self.print_documentation()
            return 0

        # Fire up engine
        print("Executing PyRunner App: {}".format(config["app_name"]))
        retcode = self.engine.initiate()

        should_notify = True
        if retcode == 0 and not config["notify_on_success"]:
            print(
                'Skipping Notification: Property "notify_on_success" is set to FALSE.'
            )
            should_notify = False
        elif retcode > 0 and not config["notify_on_fail"]:
            print('Skipping Notification: Property "notify_on_fail" is set to FALSE.')
            should_notify = False

        if should_notify:
            self.notification.emit_notification(config, self.register)

        if not config["nozip"]:
            self.zip_log_files(retcode)

        self.cleanup_log_files()

        if retcode == 0:
            self.delete_state()

        return retcode

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

    def cleanup_log_files(self):
        if config["log_retention"] < 0:
            return

        try:
            files = glob.glob("{}/*".format(config["root_log_dir"]))
            to_delete = [
                f
                for f in files
                if os.stat(f).st_mtime
                < (time.time() - (config["log_retention"] * 86400.0))
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
                config["log_dir"],
                config["app_name"],
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

    def parse_args(self, run_getopts=True):
        abort, revive = False, False

        opt_list = "c:l:n:e:x:N:D:A:t:drhiv"
        longopt_list = [
            "setup",
            "help",
            "nozip",
            "interactive",
            "abort",
            "restart",
            "version",
            "dryrun",
            "debug",
            "silent",
            "preserve-context",
            "dump-logs",
            "allow-duplicate-jobs",
            "email=",
            "email-on-fail=",
            "email-on-success=",
            "env=",
            "cvar=",
            "context=",
            "time-between-tasks=",
            "to=",
            "from=",
            "descendants=",
            "ancestors=",
            "norun=",
            "exec-only=",
            "exec-proc-name=",
            "max-procs=",
            "serde=",
            "exec-loop-interval=",
            "notify-on-fail=",
            "notify-on-success=",
            "service-exec-interval=",
            "revive",
        ]

        if run_getopts:
            try:
                opts, _ = getopt.getopt(sys.argv[1:], opt_list, longopt_list)
            except getopt.GetoptError as e:
                print(str(e))
                self.show_help()
                sys.exit(1)

            for opt, arg in opts:
                if opt == "-c":
                    config["config_file"] = arg
                elif opt == "-l":
                    config["proc_file"] = arg
                elif opt in ["-d", "--debug"]:
                    config["debug"] = True
                elif opt in ["-n", "--max-procs"]:
                    config["max_procs"] = int(arg)
                elif opt in ["-r", "--restart"]:
                    config["restart"] = True
                elif opt in ["-x", "--exec-only"]:
                    config["exec_only_list"] = [int(id) for id in arg.split(",")]
                elif opt in ["-N", "--norun"]:
                    config["exec_disable_list"] = [int(id) for id in arg.split(",")]
                elif opt in ["-D", "--from", "--descendents"]:
                    config["exec_from_id"] = int(arg)
                elif opt in ["-A", "--to", "--ancestors"]:
                    config["exec_to_id"] = int(arg)
                elif opt in ["-e", "--email"]:
                    config["email"] = arg
                elif opt == "--notify-on-fail":
                    config["notify_on_fail"] = arg
                elif opt == "--notify-on-success":
                    config["nitory_on_success"] = arg
                elif opt == "--env":
                    parts = arg.split("=")
                    os.environ[parts[0]] = parts[1]
                elif opt == "--cvar":
                    parts = arg.split("=")
                    config["cvar_list"].append((parts[0], parts[1]))
                elif opt == "--nozip":
                    config["nozip"] = True
                elif opt == "--dump-logs":
                    config["dump_logs"] = True
                elif opt == "--dryrun":
                    config["dryrun"] = True
                elif opt in ["-i", "--interactive"]:
                    self.engine.context.interactive = True
                elif opt in ["-t", "--tickrate"]:
                    config["tickrate"] = int(arg)
                elif opt in ["--time-between-tasks"]:
                    config["time_between_tasks"] = int(arg)
                elif opt in ["--preserve-context"]:
                    self.preserve_context = True
                elif opt in ["--allow-duplicate-jobs"]:
                    config["allow_duplicate_jobs"] = True
                elif opt in ["--exec-proc-name"]:
                    config["exec_proc_name"] = arg
                elif opt == "--service-exec-interval":
                    config["service_exec_interval"] = int(arg)
                elif opt == "--revive":
                    revive = True
                elif opt == "--abort":
                    abort = True
                elif opt == "--silent":
                    config["silent"] = True
                elif opt in ["--serde"]:
                    if arg.lower() == "json":
                        self.plugin_serde(serde.JsonSerDe())
                elif opt in ("-h", "--help"):
                    self.show_help()
                    sys.exit(0)
                elif opt in ("-v", "--version"):
                    print("PyRunner v{}".format(__version__))
                    sys.exit(0)
                else:
                    raise ValueError("Error during parsing of opts")

        # We need to check for and source the app_profile/config file ASAP,
        # but only after --env vars are processed
        if not config["config_file"]:
            raise RuntimeError("Config file (app_profile) has not been provided")
        config.source_config_file(config["config_file"])

        if abort:
            print(
                "Submitting ABORT signal to running job for: {}".format(
                    config["app_name"]
                )
            )
            self.signal_handler.emit(SIG_ABORT)
            sys.exit(0)

        if revive:
            print(
                "Submitting REVIVE signal to running job for: {}".format(
                    config["app_name"]
                )
            )
            self.signal_handler.emit(SIG_REVIVE)
            sys.exit(0)

        # Check if restart is possible (ctllog/ctx files exist)
        if config["restart"] and not self.is_restartable():
            config["restart"] = False

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
                     --cvar <VAR_NAME=var_value>          Provide key/value pair to initialize the Context object with prior to execution. Can provide this option multiple times.
                     --nozip                              Disable behavior which zips up all log files after job exit.
                     --dump-logs                          Enable behavior which prints all failure logs, if any, to STDOUT after job exit.
                -t,  --tickrate <num>                     Number of times per second that the executon engine should poll child processes/launch new processes. Default is 1.
                     --time-between-tasks <seconds>       Number of seconds, at minimum, that the execution engine should wait after launching a process before launching another.
                     --serde <serializer/deserializer>    Specify the process list serializer/deserializer. Default is LST.
                     --preserve-context                   Disables behavior which deletes the job's context file after successful job exit.
                     --allow-duplicate-jobs               Enables running more than 1 instance of a unique job (based on APP_NAME).
                     --abort                              Aborts running instance of a job (based on APP_NAME), if any.
                     --setup                              Run the PyRunner basic project setup.
                -v,  --version                            Print PyRunner version.
                -h,  --help                               Show help (you're reading it right now).
            """
        )
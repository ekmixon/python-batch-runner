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

import os, sys, uuid, configparser, pathlib, inspect
from datetime import datetime

__config = None

def get_config_instance():
    global __config
    if not __config:
        __config = Config()
    return __config

class Config(configparser.ConfigParser):
    def __init__(self, *args, **kwargs):
        if "interpolation" not in kwargs:
            kwargs["interpolation"] = configparser.ExtendedInterpolation()

        super().__init__(os.environ, *args, **kwargs)

        self.add_section("_")
        self["_"]["date"] = datetime.now().strftime("%Y-%m-%d")
        self["_"]["start_time"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        # Framework Defaults
        self.add_section("framework")
        if hasattr(sys, "ps1"):
            # We're in interactive mode, so we can only rely on the CWD
            self["framework"]["app_root_dir"] = os.getcwd()
        else:
            self["framework"]["app_root_dir"] = "{}/{}".format(
                os.getcwd(), inspect.stack()[1].filename
            )
        self["framework"]["app_name"] = "PyrunnerApp_{}".format(uuid.uuid4())
        self["framework"]["version"] = "0.0.0"
        self["framework"]["worker_dir"] = ""
        self["framework"]["config_dir"] = ""
        self["framework"]["temp_dir"] = ""
        self["framework"]["log_root_dir"] = ""
        self["framework"]["log_dir"] = ""
        self["framework"]["log_retention"] = "30"

        # Launch Parameters
        self.add_section("launch_params")
        self["launch_params"]["config_file"] = ""
        self["launch_params"]["proc_file"] = ""
        self["launch_params"]["restart"] = "false"
        self["launch_params"]["cvar_list"] = ""
        self["launch_params"]["exec_proc_name"] = ""
        self["launch_params"]["exec_only_list"] = ""
        self["launch_params"]["exec_disable_list"] = ""
        self["launch_params"]["exec_from_id"] = ""
        self["launch_params"]["exec_to_id"] = ""
        self["launch_params"]["nozip"] = "false"
        self["launch_params"]["dump_logs"] = "false"
        self["launch_params"]["email"] = ""
        self["launch_params"]["silent"] = "false"
        self["launch_params"]["debug"] = "false"
        self["launch_params"]["tickrate"] = "1"
        self["launch_params"]["time_between_tasks"] = "0"
        self["launch_params"]["save_interval"] = "10"
        self["launch_params"]["max_procs"] = "-1"
        self["launch_params"]["dryrun"] = "false"
        self["launch_params"]["notify_on_fail"] = "true"
        self["launch_params"]["notify_on_success"] = "true"
        self["launch_params"]["test_mode"] = "false"

        self.add_section("components")
        self["components"]["serde"] = "JsonSerDe"

    def load_cfg(self, cfg_file):
        if not os.path.isfile(cfg_file):
            raise FileNotFoundError("Config file {} does not exist".format(cfg_file))
        self.read(cfg_file)
        
        cfg_dir_path = pathlib.Path(cfg_file).parent.resolve()
        temp = os.getcwd()
        
        os.chdir(cfg_dir_path)
        self["framework"]["app_root_dir"] = str(
            pathlib.Path(self["framework"]["app_root_dir"]).resolve()
        )

        os.chdir(temp)

config = get_config_instance()
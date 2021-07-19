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

import os, re, json
import pyrunner.core.constants as constants
from pyrunner.jobspec.abstract import JobSpec
from pyrunner.core.register import NodeRegister


class JsonFileJobSpec(JobSpec):
    def load(self, proc_file, with_status=False):
        """
        Returns a NodeRegister represented by the contents of provided JSON file.

        The root object is expected to have at least a 'task' attribute, whose
        value is an inner object keyed on the Task Name. Each Task Name is additionally
        an inner object with at minimum the 'module' and 'worker' attributes.

        See <URL here> for JSON file specifications.

        Args:
          proc_file (str): The path string for the JSON file containing a valid
            Execution Graph representation.
          restart (bool, optional): Flag to indicate if input file is a restart file.
            Default: False

        Returns:
          A NodeRegister representation of the Execution Graph in the JSON file.
        """

        print("Processing Process JSON File: {}".format(proc_file))
        if not proc_file or not os.path.isfile(proc_file):
            raise FileNotFoundError("Process file {} does not exist.".format(proc_file))

        register = NodeRegister()
        with open(proc_file) as f:
            proc_obj = json.load(f)
        used_names = set()

        for name, details in proc_obj["tasks"].items():
            if name in used_names:
                raise RuntimeError(
                    "Task name {} has already been registered".format(name)
                )
            else:
                used_names.add(name)

            register.add_node(name=name, **details)

        return register

    def dump(self, proc_file, node_register, with_status=False):
        node_list = []
        
        for grp in node_register.register:
            for node in node_register.register[grp]:
                node_list.append((node, grp))
        
        node_list.sort(key=(lambda n: n[0].id))

        obj = {"tasks": dict()}
        for node, status in node_list:
            obj["tasks"][node.name] = {
                "module": node.module,
                "worker": node.worker,
                "logfile": node.logfile,
            }
            if not (
                len(node.parent_nodes) == 1
                and tuple(node.parent_nodes)[0].name == constants.ROOT_NODE_NAME
            ):
                obj["tasks"][node.name]["dependencies"] = [
                    p.name for p in node.parent_nodes
                ]
            if node.max_attempts > 1:
                obj["tasks"][node.name]["max_attempts"] = node.max_attempts
                obj["tasks"][node.name]["retry_wait_time"] = node.retry_wait_time
            if node.arguments:
                obj["tasks"][node.name]["arguments"] = node.arguments
            if node.timeout != float("inf"):
                obj["tasks"][node.name]["timeout"] = node.timeout
            if with_status:
                obj["tasks"][node.name]["status"] = status
                obj["tasks"][node.name]["elapsed_time"] = node.get_elapsed_time()

        tmp = proc_file + ".tmp"
        perm = proc_file

        with open(tmp, "w") as f:
            json.dump(obj, f, indent=4)
        if os.path.isfile(perm):
            os.unlink(perm)
        os.rename(tmp, perm)
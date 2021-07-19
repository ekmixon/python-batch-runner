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

import os, re
import pyrunner.core.constants as constants
from pyrunner.jobspec.abstract import JobSpec
from pyrunner.core.register import NodeRegister


class ListFileJobSpec(JobSpec):
    def load(self, proc_file, with_status=False):
        print("Processing Process List File: {}".format(proc_file))
        if not proc_file or not os.path.isfile(proc_file):
            raise FileNotFoundError("Process file {} does not exist.".format(proc_file))

        register = NodeRegister()
        pipe_pattern = re.compile(r"""((?:[^|"']|"[^"]*"|'[^']*')+)""")
        comma_pattern = re.compile(r"""((?:[^,"']|"[^"]*"|'[^']*')+)""")

        with open(proc_file) as f:
            proc_list = f.read().splitlines()

        if not proc_list:
            raise ValueError("No information read from process list file")

        used_ids = set()

        for proc in proc_list:
            proc = proc.strip()

            # Skip Comments and Empty Lines
            if not proc or proc[0] == "#":
                continue

            details = [
                x.strip(" |") for x in pipe_pattern.split(proc)[1:-1] if x != "|"
            ]

            node_id = int(details[0])
            if node_id in used_ids:
                return False
            else:
                used_ids.add(node_id)

            dependencies = [int(x) for x in details[1].split(",")]

            if with_status:
                register.add_node(
                    id=node_id,
                    dependencies=dependencies,
                    max_attempts=details[2],
                    retry_wait_time=details[3],
                    status=details[4]
                    if details[4]
                    in [constants.STATUS_COMPLETED, constants.STATUS_NORUN]
                    else constants.STATUS_PENDING,
                    name=details[6],
                    module=details[7],
                    worker=details[8],
                    argv=[
                        s.strip('"')
                        if s.strip().startswith('"') and s.strip().endswith('"')
                        else s.strip()
                        for s in comma_pattern.split(details[9])[1::2]
                    ]
                    if len(details) > 9
                    else None,
                    logfile=details[10] if len(details) > 10 else None,
                    named_deps=False,
                )
            else:
                register.add_node(
                    id=node_id,
                    dependencies=dependencies,
                    max_attempts=details[2],
                    retry_wait_time=details[3],
                    name=details[4],
                    module=details[5],
                    worker=details[6],
                    argv=[
                        s.strip('"')
                        if s.strip().startswith('"') and s.strip().endswith('"')
                        else s.strip()
                        for s in comma_pattern.split(details[7])[1::2]
                    ]
                    if len(details) > 7
                    else None,
                    logfile=details[8] if len(details) > 8 else None,
                    named_deps=False,
                )

        return register

    def get_ctllog_line(self, node, status=None):
        parent_id_list = [str(x.id) for x in node.parent_nodes]
        parent_id_str = ",".join(parent_id_list) if parent_id_list else "-1"

        if status:
            return "|".join(
                [
                    str(node.id),
                    parent_id_str,
                    str(node.max_attempts),
                    str(node.retry_wait_time),
                    status,
                    node.get_elapsed_time(),
                    node.name,
                    node.module,
                    node.worker,
                    ",".join(node.argv),
                    node.logfile,
                ]
            )
        else:
            return "|".join(
                [
                    str(node.id),
                    parent_id_str,
                    str(node.max_attempts),
                    str(node.retry_wait_time),
                    node.name,
                    node.module,
                    node.worker,
                    ",".join(node.argv),
                    node.logfile,
                ]
            )

    def dump(self, proc_file, node_register, with_status=False):
        node_list = []
        
        for grp in node_register.register:
            for node in node_register.register[grp]:
                node_list.append((node, grp))
        
        node_list.sort(key=(lambda n: n[0].id))

        if with_status:
            body = "{}\n\n".format(constants.HEADER_PYTHON) + "\n".join(
                [self.get_ctllog_line(node, status) for node, status in node_list]
            )
        else:
            body = "{}\n\n".format(constants.HEADER_PYTHON) + "\n".join(
                [self.get_ctllog_line(node) for node, _ in node_list]
            )

        tmp = proc_file + ".tmp"
        perm = proc_file

        with open(tmp, "w") as f:
            f.write(body)
        if os.path.isfile(perm):
            os.unlink(perm)
        os.rename(tmp, perm)
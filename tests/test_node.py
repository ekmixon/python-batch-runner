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

import pytest, pathlib, sys

from pyrunner.core.config import config
from pyrunner.core.node import ExecutionNode

sys.path.append("/Users/nathan/projects/python/python-batch-runner/tests/python")
#config['worker_dir'] = "/Users/nathan/projects/python/python-batch-runner/tests/python"
config["framework"]["worker_dir"] = str(pathlib.Path(__file__).parent.absolute() / "python")
print(config["framework"]["worker_dir"])


@pytest.fixture
def node(module=None, worker=None):
    """Returns a root ExecutionNode with 1 id"""
    node = ExecutionNode(1)
    node.name = "Test"
    if module and worker:
        node.module = module
        node.worker = worker
    return node


@pytest.mark.parametrize(
    "module, worker, exp_retcode", [("sample", "SayHello", 0), ("sample", "FailMe", 1)]
)
def test_return_code(node, module, worker, exp_retcode):
    print(config["framework"]["worker_dir"])
    node.module = module
    node.worker = worker
    node.execute()
    rc = node.poll(True)
    assert rc == exp_retcode


@pytest.mark.parametrize(
    "module, worker",
    [
        ("exceptions", "ThrowValueError"),
        ("exceptions", "ThrowRuntimeError"),
        ("exceptions", "InvalidInt"),
    ],
)
def test_exception_returns_nonzero(node, module, worker):
    node.module = module
    node.worker = worker
    node.execute()
    rc = node.poll(True)
    assert rc > 0


@pytest.mark.parametrize("attempts", [1, 2, 3, 4, 5])
def test_num_retries(node, attempts):
    node.module = "sample"
    node.worker = "FailMe"
    node.max_attempts = attempts
    node.retry_wait_time = 0
    node.execute()
    while (node.poll(True) or -1) < 0:
        node.execute()
    assert node._attempts == attempts
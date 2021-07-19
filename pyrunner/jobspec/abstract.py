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

import os
from abc import ABCMeta, abstractmethod


class JobSpec(metaclass=ABCMeta):
    """
    Implementations of this abstract class serve to translate between a
    NodeRegister and it's off-memory/persistant representation on the file system
    or elsewhere.
    """

    @abstractmethod
    def dump(self, proc_file, node_register, with_status):
        """
        This method must be implemented in the child class to translate a provided
        NodeRegister instance to it's target off-memory representation.
        """
        pass

    @abstractmethod
    def load(self, proc_file, with_status=False):
        """
        This method must be implemented in the child class to translate it's
        off-memory representation to a NodeRegister instance.
        """
        pass
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

import time, os, pickle
from collections import deque
from multiprocessing import Manager

__context = None
__manager = None
__shared_dict = None
__shared_queue = None

def get_context_instance():
    global __context, __manager, __shared_dict, __shared_queue
    if not __context:
        __manager = Manager()
        __shared_dict = __manager.dict()
        __shared_queue = __manager.Queue()
        __context = Context(__shared_dict, __shared_queue)
    return __context

class Context:
    """
    Stores dictionary and queue objects to be shared across all processes.

    Accepts various forms of a dictionary and queue object. However, during
    normal execution, these will be multiprocessing.Manager data structures
    in order to allow data sharing across unique processes (within current
    app/job instance).

    Attributes are accessed in the same manner as attributes/values in a dict.

    Attributes:
      interactive: Boolean flag to specify if app is executed in 'interactive' mode.
    """

    def __init__(self, shared_dict, shared_queue):
        self.shared_dict = shared_dict
        self.shared_queue = shared_queue
        self.interactive = False
        self._iter_keys = None

    def __iter__(self):
        self._iter_keys = deque(self.shared_dict.keys())
        return self

    def __next__(self):
        if not self._iter_keys:
            raise StopIteration
        else:
            return self._iter_keys.popleft()

    def __getitem__(self, key):
        return self.shared_dict[key]

    def __setitem__(self, key, value):
        self.shared_dict[key] = value

    def __delitem__(self, key):
        del self.shared_dict[key]

    def __contains__(self, key):
        return key in self.shared_dict

    def items(self):
        return self.shared_dict.items()

    @property
    def keys(self):
        return self.shared_dict.keys()

    def has_key(self, key):
        return key in self.shared_dict

    def set(self, key, value):
        self.shared_dict[key] = value

    def get(self, key, default=None):
        """
        Retrieves value for provided attribute, if any.

        Similar to the .get() method for a dict object. If 'interactive' is set
        to True and a non-existent attribute is requested, execution for the
        calling Worker will pause and wait for input from STDIN to use as
        the return value, instead of None.

        Returns:
          Stored value for key or value provided via STDIN if 'interactive'
          attribute is True. Otherwise None.
        """
        if self.interactive and not default and key not in self.shared_dict:
            self.shared_queue.put(key)
            while key not in self.shared_dict:
                time.sleep(0.5)

        return self.shared_dict.get(key, default)
    
    def load_from_file(self, file_path):
        """
        Loads context from a pickled file.

        Args:
          file_path: Path to file to load context from.
        """
        loaded = pickle.load(open(file_path, "rb"))
        for k, v in loaded.items():
            self.shared_dict[k] = v

    def save_to_file(self, file_path):
        """
        Saves the current context to a file.

        Args:
          file_path: Path to file to save context to.
        """
        tmp = file_path + ".tmp"
        perm = file_path
        pickle.dump(self.shared_dict.copy(), open(tmp, "wb"))
        if os.path.isfile(perm):
            os.unlink(perm)
        os.rename(tmp, perm)
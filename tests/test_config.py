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

import pytest
import os
from pathlib import Path
from datetime import datetime

from pyrunner.core.config import Config

cur_date = datetime.now().strftime("%Y-%m-%d")
abs_dir_path = os.path.dirname(os.path.realpath(__file__))
cfg_file = Path("{}/config/app.cfg".format(abs_dir_path))


@pytest.fixture
def cfg():
    """Returns a Config object loaded with test_profile"""
    cfg = Config()
    cfg.load_cfg(cfg_file)
    return cfg


@pytest.mark.parametrize(
    "section, key, value",
    [
        ("framework", "app_name", "TestApplication"),
        ("framework", "app_root_dir", abs_dir_path),
        ("framework", "config_dir", "{}/config".format(abs_dir_path)),
        ("framework", "temp_dir", "{}/temp".format(abs_dir_path)),
        ("framework", "data_dir", "{}/data".format(abs_dir_path)),
        ("framework", "log_root_dir", "{}/logs".format(abs_dir_path)),
        ("framework", "worker_dir", "{}/workers".format(abs_dir_path)),
        ("framework", "log_retention", "1"),
        (
            "framework",
            "log_dir",
            "{}/logs/{}".format(abs_dir_path, cur_date),
        ),
        ("custom", "custom_variable_1", "my custom variable 1"),
        ("custom", "interpolation_variable", "TestApplication - Custom"),
    ],
)
def test_load_config(cfg, section, key, value):
    """Ensure that the config file properly loads all vars from a config file."""
    assert cfg[section][key] == value

@pytest.mark.parametrize(
    "section, key, value",
    [
        ("framework", "app_name", "SomeWildName"),
        ("framework", "app_root_dir", "/my/root/directory"),
        ("framework", "log_dir", "/i/can/be/elsewhere"),
    ],
)
def test_config_modify_attr(cfg, section, key, value):
    """Test simple config valuue modifications."""
    cfg[section][key] = value
    assert (cfg[section][key] == value)


def test_attribute_error(cfg):
    with pytest.raises(AttributeError):
        cfg.unreal_var


def test_section_error(cfg):
    with pytest.raises(KeyError):
        cfg["unreal_var"]


def test_section_attribute_error(cfg):
    with pytest.raises(KeyError):
        cfg["framework"]["unreal_var"]

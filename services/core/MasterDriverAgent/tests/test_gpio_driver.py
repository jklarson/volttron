# -*- coding: utf-8 -*- {{{
# vim: set fenc=utf-8 ft=python sw=4 ts=4 sts=4 et:
#
# Copyright 2020, Battelle Memorial Institute.
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
# This material was prepared as an account of work sponsored by an agency of
# the United States Government. Neither the United States Government nor the
# United States Department of Energy, nor Battelle, nor any of their
# employees, nor any jurisdiction or organization that has cooperated in the
# development of these materials, makes any warranty, express or
# implied, or assumes any legal liability or responsibility for the accuracy,
# completeness, or usefulness or any information, apparatus, product,
# software, or process disclosed, or represents that its use would not infringe
# privately owned rights. Reference herein to any specific commercial product,
# process, or service by trade name, trademark, manufacturer, or otherwise
# does not necessarily constitute or imply its endorsement, recommendation, or
# favoring by the United States Government or any agency thereof, or
# Battelle Memorial Institute. The views and opinions of authors expressed
# herein do not necessarily state or reflect those of the
# United States Government or any agency thereof.
#
# PACIFIC NORTHWEST NATIONAL LABORATORY operated by
# BATTELLE for the UNITED STATES DEPARTMENT OF ENERGY
# under Contract DE-AC05-76RL01830
# }}}

import time
import logging
import threading
import pytest
import json
from services.core.MasterDriverAgent.master_driver.interfaces.gpio import Interface

_log = logging.getLogger(__name__)

# Contents of driver config.
config_dict = {"pin_factory": "mock"}

# Registry config.
registry_config_str = [{"Point Name": "Solenoid","Pin": 24,"Device Type": "output",
                        "Active High": "TRUE",
                        "Initial Value": "TRUE"},
                       {"Point Name": "flowSensor",
                        "Pin": 12,
                        "Device Type": "digital_input",
                        "Pull Up": "TRUE"},
                       {"Point Name": "flowLED",
                        "Pin": 13,
                        "Device Type": "digital_output",
                        "Active High": "TRUE",
                        "Initial Value": "TRUE"},
                       {"Point Name": "abortButton",
                        "Pin": 16,
                        "Device Type": "digital_input",
                        "Pull Up": "FALSE"}]

# Invalid configurations.
failing_configs = [({"pin_factory": "random_factory"}, registry_config_str),
                   (config_dict, [{"Point Name": "test", "Pin": "28"}]),
                   (config_dict, [{"Point Name": "test", "Pin": "test"}]),
                   (config_dict, [{"Point Name": "test", "Pin": "5", "Device Type": "device"}]),
                   (config_dict, [{"Point Name": "test", "Pin": "28", "Device Type": "output",
                                  "Active High": "None"}]),
                   (config_dict, [{"Point Name": "test", "Pin": "test", "Initial Value": "None"}]),
                   (config_dict, [{"Point Name": "test", "Pin": "5", "Device Type": "digital input"}]),
                   (config_dict, [{"Point Name": "test", "Pin": "5", "Device Type": "digital input",
                                  "Pull Up": "None"}]),
                   (config_dict, [{"Point Name": "test", "Pin": "5", "Device Type": "digital input",
                                  "Active High": "None"}])]


def drive_low(abort_pin):
    time.sleep(2)
    abort_pin.drive_low()
    time.sleep(2)
    return time.time()


def drive_high(abort_pin):
    time.sleep(2)
    abort_pin.drive_high()
    time.sleep(2)
    return time.time()


@pytest.fixture()
def test_configuration():
    test = Interface()
    test.configure(config_dict, registry_config_str)
    assert test._scrape_all() == {"Solenoid": 0, "flowSensor": 0, "flowLED": 0, "abortButton": 0}
    return test


@pytest.mark.parametrize("driv_config, reg_config", failing_configs)
def test_configuration_failure(driv_config, reg_config):
    test = Interface()
    with pytest.raises(ValueError):
        test.configure(driv_config, reg_config)


def test_registers_get(test_configuration):
    assert test_configuration.get_point("Solenoid") == 0
    test_configuration.pin_factory.reset()


def test_registers_set(test_configuration):
    test_configuration._set_point("Solenoid", "1")
    assert test_configuration.get_point("Solenoid") == 1
    with pytest.raises(ValueError):
        test_configuration._set_point("Solenoid", "test")
    test_configuration.pin_factory.reset()


def test_output_registers(test_configuration):
    # Test output actions

    # Test on
    solenoid_pin = test_configuration.pin_factory.pin(24)
    point_name = "Solenoid"
    action_dictionary = {
        "action": "on"
    }
    test_configuration._set_point(point_name, action_dictionary)
    assert solenoid_pin._get_state() == 1

    # Test off
    action_dictionary = {
        "action": "off"
    }
    test_configuration._set_point(point_name, action_dictionary)
    assert solenoid_pin._get_state() == 0

    # Test toggle
    action_dictionary = {
        "action": "toggle"
    }
    test_configuration._set_point(point_name, action_dictionary)
    assert solenoid_pin._get_state() == 1

    # Test active_high
    action_dictionary = {
        "action": "set_active_high",
        "active_high": "False"
    }
    test_configuration._set_point(point_name, action_dictionary)
    with pytest.raises(ValueError):
        action_dictionary = {
            "action": "set_active_high"
        }
        test_configuration._set_point(point_name, action_dictionary)
    with pytest.raises(ValueError):
        action_dictionary = {
            "action": "set_active_high",
            "active_high": "test"
        }
        test_configuration._set_point(point_name, action_dictionary)
    action_dictionary = {
        "action": "get_active_high"
    }
    assert not test_configuration._set_point(point_name, action_dictionary)

    # Test invalid action.
    with pytest.raises(ValueError):
        action_dictionary = {
            "action": "invalid"
        }
        test_configuration._set_point(point_name, action_dictionary)

    test_configuration.pin_factory.reset()


def test_digital_output_registers(test_configuration):
    # Test digital output actions

    # Test on
    flowLED_pin = test_configuration.pin_factory.pin(13)
    point_name = "flowLED"
    action_dictionary = {
        "action": "on"
    }
    test_configuration._set_point(point_name, action_dictionary)
    assert flowLED_pin._get_state() == 1

    # Test off
    action_dictionary = {
        "action": "off"
    }
    test_configuration._set_point(point_name, action_dictionary)
    assert flowLED_pin._get_state() == 0

    # Test toggle
    action_dictionary = {
        "action": "toggle"
    }
    test_configuration._set_point(point_name, action_dictionary)
    assert flowLED_pin._get_state() == 1

    # Test active_high
    action_dictionary = {
        "action": "set_active_high",
        "active_high": "False"
    }
    test_configuration._set_point(point_name, action_dictionary)
    with pytest.raises(ValueError):
        action_dictionary = {
            "action": "set_active_high"
        }
        test_configuration._set_point(point_name, action_dictionary)
    with pytest.raises(ValueError):
        action_dictionary = {
            "action": "set_active_high",
            "active_high": "test"
        }
        test_configuration._set_point(point_name, action_dictionary)
    action_dictionary = {
        "action": "get_active_high"
    }
    assert not test_configuration._set_point(point_name, action_dictionary)

    # Test invalid action.
    with pytest.raises(ValueError):
        action_dictionary = {
            "action": "invalid"
        }
        test_configuration._set_point(point_name, action_dictionary)

    test_configuration.pin_factory.reset()


def test_digital_input_registers(test_configuration):
    #Test digital input register

    # Test wait for inactive trigger
    point_name = "abortButton"
    action_dictionary = {
        "action": "wait_for_inactive",
        "timeout": 20.0
    }
    # Begin with pin as active
    abort_pin = test_configuration.pin_factory.pin(16)
    abort_pin.drive_high()
    # Set pin inactive in 2 seconds
    pin_thread = threading.Thread(target=drive_low, args=([abort_pin]))
    pin_thread.start()
    time_start = time.time()
    test_configuration._set_point(point_name, action_dictionary)
    assert (time.time() - time_start) < 4

    # Test wait for active
    action_dictionary = {
        "action": "wait_for_active",
        "timeout": 20.0
    }
    # Set pin active in 2 seconds
    pin_thread = threading.Thread(target=drive_high, args=([abort_pin]))
    pin_thread.start()
    time_start = time.time()
    test_configuration._set_point(point_name, action_dictionary)
    assert (time.time() - time_start) < 4

    # Test active time
    action_dictionary = {
        "action": "active_time"
    }
    time.sleep(5)
    assert test_configuration._set_point(point_name, action_dictionary) >= 5

    # Test inactive time
    action_dictionary = {
        "action": "inactive_time"
    }
    assert not test_configuration._set_point(point_name, action_dictionary)

    # Test invalid action.
    with pytest.raises(ValueError):
        action_dictionary = {
            "action": "invalid"
        }
        test_configuration._set_point(point_name, action_dictionary)


# -*- coding: utf-8 -*- {{{
# vim: set fenc=utf-8 ft=python sw=4 ts=4 sts=4 et:
#
# Copyright 2019, Battelle Memorial Institute.
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


import gevent
import pytest
import re
import sqlite3
from datetime import datetime, timedelta
from mock import MagicMock

from volttron.platform import jsonapi, get_examples, get_services_core
from volttron.platform.agent.known_identities import CONFIGURATION_STORE, PLATFORM_DRIVER


@pytest.fixture(scope="module")
def query_agent(request, volttron_instance):
    # Start a fake agent to query the historian agent
    agent = volttron_instance.build_agent()
    agent.poll_callback = MagicMock(name="poll_callback")
    # subscribe to devices topics
    agent.vip.pubsub.subscribe(peer='pubsub', prefix="devices", callback=agent.poll_callback).get()

    # 2: add a tear down method to stop the fake agent that published to message bus
    def stop_agent():
        print("In teardown method of query_agent")
        agent.core.stop()

    request.addfinalizer(stop_agent)
    return agent


@pytest.fixture(scope="module")
def master_driver(request, volttron_instance, query_agent):
    master_driver = volttron_instance.install_agent(
        agent_dir=get_services_core("MasterDriverAgent"),
        start=False,
        config_file={
            "publish_breadth_first_all": False,
            "publish_depth_first": False,
            "publish_breadth_first": False
        })

    driver_config = jsonapi.load(open(get_examples("configurations/drivers/fake.config")))
    with open("configurations/drivers/fake.csv") as registry_file:
        registry_string = registry_file.read()
    registry_path = re.search("(?!config:\/\/)[a-zA-z]+\.csv", driver_config.get("registry_config"))

    query_agent.vip.rpc.call(CONFIGURATION_STORE, "manage_store", PLATFORM_DRIVER, "devices/campus/building/fake",
                             driver_config)
    query_agent.vip.rpc.call(CONFIGURATION_STORE, "manage_store", PLATFORM_DRIVER, registry_path, registry_string,
                             config_type="csv")
    volttron_instance.start_agent(master_driver)
    gevent.wait(1)
    assert volttron_instance.is_agent_running(master_driver)

    def stop():
        """Stop master driver agent
        """
        volttron_instance.stop_agent(master_driver)

    request.addfinalizer(stop)
    return master_driver


@pytest.fixture(scope="module")
def archive_historian(request, volttron_instance):
    # install a copy of the archive historian using the default config
    config = jsonapi.load(open("SQLiteHistorian/config", "r"))
    archive_historian = volttron_instance.install_agent(
        agent_dir="SQLiteArchiveHistorian",
        start=True,
        config_file=config)
    gevent.wait(1)
    assert volttron_instance.is_agent_running(master_driver)

    def stop():
        # stop running the agent
        volttron_instance.stop_agent(archive_historian)

    request.addfinalizer(stop)

    return archive_historian


# TODO "copy" tests for other methods into here

def test_manage_db_size_success(query_agent, master_driver, archive_historian):
    # reset query agent mock
    # wait 60 seconds (so that the database will rollover the minimal amount of time
    # check the rollover db was created
    # check new db is created
    # check rollover db and mock are the same
    # check new db does not have the same data
    pass

# TODO db size fail?

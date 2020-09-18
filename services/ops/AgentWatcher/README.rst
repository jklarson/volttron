.. _Agent-Watcher-Agent:

===================
Agent Watcher Agent
===================

The Agent Watcher is used to monitor agents running on a VOLTTRON instance.  Specifically it monitors whether a set of \
VIP identities (peers) are connected to the instance.   If any of the peers in the set are not present then an alert
will be sent.


Configuration
=============

The agent watcher configuration requires a list of VIP identities of agents installed on the platform.

.. code-block::

    {
        # AgentWatcher will send an alert if any of the following VIP identities are not running on the platform
        "watchlist": [
            "platform.driver",
            "platform.historian"
        ],
        # Time to wait between agent running checks, defaults to 10 seconds.
        "check-period": 10
        #
    }

Agents with an identity beginning with "platform." are included in the known identities file.  These agents are agents
which are commonly used by users in deployments (such as the master driver - "platform.driver) or are used by the
platform for internal management.  For more information, review the :ref:`known identities <VIP-Known-Identities>` docs
or check out the known identities file at `volttron/platform/agent/known-identities.py` in the VOLTTRON repository.

For agents not featured in known identities, input the identity given to the agent during install.  This identity can
be found with the ``vctl status`` command.

.. code-block:: bash

   vctl status




Installation
============

To install the AgentWatcher on a running platform:

#. Copy the the example config to your config directory (optional)

.. code-block:: bash

   cp services/ops/AgentWatcher/config configs

#. Run the install-agent.py script

   If not already activated, run the following command in the root directory of the VOLTTRON repository:

   .. code-block:: bash

      source env/bin/activate

   Then run the script:

   .. code-block:: bash

      python scripts/install-agent.py -s services/ops/AgentWatcher -c <path to config> -t <optional tag>

#. Confirm installation

   When the agent has been installed, the ``vctl status`` command can be used to view the agent status

   .. code-block:: bash

      vctl status





Usage
=====

To start the agent process run the ``vctl start`` command, optionally using the tag (shown in ``vctl status``)

.. code-block:: bash

   vctl start <tag or uuid>

When the agent has successfully started, the status should show ``GOOD``

.. code-block:: console



Based on the interval specified in the config (or 10 seconds by default) the agent will check the watchlist.  If the
agents in the watch list are performing as expected, nothing extraordinary will occur.  If an agent in the watchlist
is not running, the AgentWatcher will issue an alert.

.. code-block:: console



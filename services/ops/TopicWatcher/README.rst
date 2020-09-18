.. _Topic-Watcher-Agent:

===================
Topic Watcher Agent
===================

The Topic Watcher Agent listens to a set of configured topics and publishes an alert if they are not published within
some time limit.  In addition to "standard" topics the Topic Watcher Agent supports inspecting device `all` topics.
This can be useful when a device contains volatile points that may not be published.


Requirements
============

The Topic Watcher agent requirements are included with the Python3 standard library.  All that is required to build
this agent is to have followed the steps correctly during :ref:`platform installation <Platform-Installation>`.


Configuration
=============

The following is an example Topic Watcher configuration.

.. code-block::

   {
       "publish-settings":
       {
           "publish-local": false,
           "publish-remote": true,
           "remote":
           {
               "serverkey": "Olx7Y7XZvSGmHDppsQKvG7BucOH8vgkRlQGZzzh5nHs",
               "vip-address": "tcp://127.0.0.1:23916",
               "identity": "remote.topic_watcher"
           }
       },
       "group1": {
           "devices/fakedriver0/all": 10
       },

       "device_group": {
           "devices/fakedriver1/all": {
               "seconds": 10,
               "points": ["temperature", "PowerState"]
           }
       }
   }

Configuration item descriptions:

- *publish-settings*:  This object is used to determine how Topic Alerts are published to a message bus.

   - publish








Topics are organized by groups.  Any alerts raised will summarize all missing topics in the group.

Individual topics have two configuration options. For standard topics
configuration consists of a key value pair of the topic to its time limit.

The other option is for `all` publishes. The topic key is paired with a
dictionary that has two keys, `"seconds"` and `"points"`. `"seconds"` is the
topic's time limit and `"points"` is a list of points to watch.

.. code-block:: python

    {
        "groupname": {
            "devices/fakedriver0/all": 10,

            "devices/fakedriver1/all": {
                "seconds": 10,
                "points": ["temperature", "PowerState"]
            }
        }
    }

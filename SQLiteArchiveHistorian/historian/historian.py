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


import logging
import sys
import threading
from services.core.SQLHistorian.sqlhistorian.historian import MaskedString
from SQLiteArchiveHistorian.historian.sqlitefuncts_archiver import SQLiteArchiverFuncts
from volttron.platform.agent import utils
from volttron.platform.agent.base_historian import BaseHistorian
from volttron.utils.docs import doc_inherit

_log = logging.getLogger(__name__)
utils.setup_logging()
__version__ = "1.0"


def sqlite_archive(config_path, **kwargs):
    """Parses the SQLiteArchiveHistorian agent configuration and returns an instance of
    the agent created using that configuration.
    :param config_path: Path to a configuration file.
    :type config_path: str
    :returns: SqliteArchive
    :rtype: SqliteArchive
    """
    try:
        config = utils.load_config(config_path)
    except Exception:
        config = {}

    connection = config.get('connection', None)
    assert connection is not None

    params = connection.get('params', None)
    assert params is not None

    # Avoid printing passwords in the debug message
    for key in ['pass', 'passwd', 'password', 'pw']:
        try:
            params[key] = MaskedString(params[key])
        except KeyError:
            pass

    utils.update_kwargs_with_config(kwargs, config)
    return SqliteArchiveHistorian(**kwargs)


class SqliteArchiveHistorian(BaseHistorian):
    """
    Document agent constructor here.
    """

    def version(self):
        return __version__

    def __init__(self, connection, tables_def=None, archive_period="", **kwargs):
        self.connection = connection
        self.tables_def, self.table_names = self.parse_table_def(tables_def)
        self.archive_period_units = archive_period[-1:]
        self.archive_period = int(archive_period[:-1])

        self.topic_id_map = {}
        self.topic_name_map = {}
        self.topic_meta = {}
        self.agg_topic_id_map = {}

        # As the ArchiveHistorian is not queryable, a "main thread" is not required
        self.bg_thread_dbutils = None
        super(SqliteArchiveHistorian, self).__init__(**kwargs)

    @doc_inherit
    def historian_setup(self):
        thread_name = threading.currentThread().getName()
        _log.debug("historian_setup on Thread: {}".format(thread_name))
        self.bg_thread_dbutils = SQLiteArchiverFuncts(self.connection['params'],
                                                      self.table_names,
                                                      self.archive_period,
                                                      self.archive_period_units)

        if not self._readonly:
            self.bg_thread_dbutils.setup_historian_tables()

        topic_id_map, topic_name_map = self.bg_thread_dbutils.get_topic_map()
        self.topic_id_map.update(topic_id_map)
        self.topic_name_map.update(topic_name_map)
        self.agg_topic_id_map = self.bg_thread_dbutils.get_agg_topic_map()

    def record_table_definitions(self, meta_table_name):
        self.bg_thread_dbutils.record_table_definitions(self.tables_def, meta_table_name)

    def manage_db_size(self, history_limit_timestamp, storage_limit_gb):
        """
        Optional function to manage database size.
        """
        self.bg_thread_dbutils.manage_db_size(history_limit_timestamp, storage_limit_gb)

    @doc_inherit
    def publish_to_historian(self, to_publish_list):
        try:
            published = 0
            with self.bg_thread_dbutils.bulk_insert() as insert_data:
                for x in to_publish_list:
                    ts = x['timestamp']
                    topic = x['topic']
                    value = x['value']
                    meta = x['meta']

                    # look at the topics that are stored in the database already to see if this topic has a value
                    lowercase_name = topic.lower()
                    topic_id = self.topic_id_map.get(lowercase_name, None)
                    db_topic_name = self.topic_name_map.get(lowercase_name,
                                                            None)
                    if topic_id is None:
                        # _log.debug('Inserting topic: {}'.format(topic))
                        # Insert topic name as is in db
                        topic_id = self.bg_thread_dbutils.insert_topic(topic)
                        # user lower case topic name when storing in map for case insensitive comparison
                        self.topic_id_map[lowercase_name] = topic_id
                        self.topic_name_map[lowercase_name] = topic
                    elif db_topic_name != topic:
                        self.bg_thread_dbutils.update_topic(topic, topic_id)
                        self.topic_name_map[lowercase_name] = topic

                    old_meta = self.topic_meta.get(topic_id, {})
                    if set(old_meta.items()) != set(meta.items()):
                        self.bg_thread_dbutils.insert_meta(topic_id, meta)
                        self.topic_meta[topic_id] = meta

                    if insert_data(ts, topic_id, value):
                        published += 1

            if published:
                if self.bg_thread_dbutils.commit():
                    self.report_all_handled()
                else:
                    _log.debug('Commit error. Rolling back {} values.'.format(published))
                    self.bg_thread_dbutils.rollback()
            else:
                _log.debug('Unable to publish {}'.format(len(to_publish_list)))
        except Exception as e:
            # TODO Unable to send alert from here
            # if isinstance(e, ConnectionError):
            #     _log.debug("Sending alert. Exception {}".format(e.args))
            #     err_message = "Unable to connect to database. Exception:{}".format(e.args)
            #     alert_id = DB_CONNECTION_FAILURE
            # else:
            #     err_message = "Unknown exception when publishing data. Exception: {}".format(e.args)
            #     alert_id = ERROR_PUBLISHING_DATA
            # self.vip.health.set_status(STATUS_BAD, err_message)
            # status = Status.from_json(self.vip.health.get_status())
            # self.vip.health.send_alert(alert_id, status)
            self.bg_thread_dbutils.rollback()
            # Raise to the platform so it is logged properly.
            raise

    def query_topic_list(self):
        """
        Unimplemented method stub.
        """
        raise NotImplementedError()

    def query_topics_by_pattern(self, topic_pattern):
        """
        Unimplemented method stub.
        """
        raise NotImplementedError()

    def query_topics_metadata(self, topics):
        """
        Unimplemented method stub.
        """
        raise NotImplementedError()

    def query_aggregate_topics(self):
        """
        Unimplemented method stub.
        """
        raise NotImplementedError()

    def query_historian(self, topic, start=None, end=None, agg_type=None, agg_period=None, skip=0, count=None,
                        order="FIRST_TO_LAST"):
        """
        Unimplemented method stub.
        """
        raise NotImplementedError()


def main():
    """Main method called to start the agent."""
    utils.vip_main(sqlite_archive, version=__version__)


if __name__ == '__main__':
    # Entry point for script
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        pass

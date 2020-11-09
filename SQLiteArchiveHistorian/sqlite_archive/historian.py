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
from services.core.SQLHistorian.sqlhistorian.historian import SQLHistorian, MaskedString
from SQLiteArchiveHistorian.sqlite_archive.sqlitefuncts_archiver import SQLiteArchiverFuncts
from volttron.platform.agent import utils

_log = logging.getLogger(__name__)
utils.setup_logging()
__version__ = "0.1"


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


# TODO are there other methods to override, or additional/different configuration to do
class SqliteArchiveHistorian(SQLHistorian):
    """
    Document agent constructor here.
    """

    def __init__(self, connection, tables_def=None, archive_period="", **kwargs):
        self.connection = connection
        self.tables_def, self.table_names = self.parse_table_def(tables_def)
        self.archive_period_units = archive_period[-1:]
        self.archive_period = int(archive_period[:-1])
        # Create two instance so connection is shared within a single thread.
        # This is because sqlite only supports sharing of connection within
        # a single thread.
        # historian_setup and publish_to_historian happens in background thread
        # everything else happens in the MainThread

        # One utils class instance( hence one db connection) for main thread
        self.main_thread_dbutils = SQLiteArchiverFuncts(self.connection['params'], self.table_names)
        # One utils class instance( hence one db connection) for main thread
        # this gets initialized in the bg_thread within historian_setup
        self.bg_thread_dbutils = None
        super(SQLHistorian, self).__init__(**kwargs)


def main():
    """Main method called to start the agent."""
    utils.vip_main(sqlite_archive, version=__version__)


if __name__ == '__main__':
    # Entry point for script
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        pass

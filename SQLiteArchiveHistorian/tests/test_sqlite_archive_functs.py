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


import datetime
import pytest
import os
import re
import shutil
from SQLiteArchiveHistorian.historian.sqlitefuncts_archiver import SQLiteArchiverFuncts

default_table_def = {
    "table_prefix": "",
    "data_table": "data",
    "topics_table": "topics",
    "meta_table": "meta",
    "agg_meta_table": "aggregate_meta",
    "agg_topics_table": "aggregate_topics"
}

params = {
    "database": "historian_test.sqlite"
}


@pytest.fixture(scope='module',
                params=[
                    dict(archive_period=5, archive_period_units='M'),
                    dict(archive_period=24, archive_period_units='h'),
                    dict(archive_period=7, archive_period_units='d'),
                    dict(archive_period=1, archive_period_units='w'),
                    dict(archive_period=1, archive_period_units='m')
                ])
def sqlite_archiver_success(request):
    archiver = SQLiteArchiverFuncts(params, default_table_def,
                                    archive_period=request.param['archive_period'],
                                    archive_period_units=request.param['archive_period_units'])
    archiver.setup_historian_tables()

    if request.param['archive_period_units'] == 'w':
        assert archiver.archive_period_units == 'd'
        assert archiver.archive_period.days == request.param['archive_period'] * 7
    assert os.path.isfile(archiver._database_name)

    show_tables = """SELECT 
                     name
                     FROM 
                         sqlite_master 
                     WHERE 
                         type ='table' AND 
                         name NOT LIKE 'sqlite_%';"""

    cursor = archiver.cursor()
    cursor.execute(show_tables)
    loaded_tables = cursor.fetchall()
    assert {'data', 'topics', 'meta'} == {table[0] for table in loaded_tables}

    yield archiver

    archiver.close()
    db_dir = os.path.dirname(archiver._database_name)
    shutil.rmtree(db_dir)


def test_manage_db_size_success(sqlite_archiver_success):
    # determine if a "default" database has been created
    db_dir = os.path.dirname(sqlite_archiver_success._database_name)
    # checkout if the dir contains what should exist before manage_db_size
    db_files = [os.path.join(db_dir, path) for path in os.listdir(db_dir)]
    assert len(db_files) == 1
    assert params.get("database") in db_files

    # immediately trigger a rollover which should occur before the update frequency has been passed
    db_files = [os.path.join(db_dir, path) for path in os.listdir(db_dir)]
    assert len(db_files) == 1
    assert params.get("database") in db_files

    # set the next update time to something immediate so we can trigger a rollover
    now = datetime.datetime.now()
    sqlite_archiver_success.next_archive_time = now
    # get the timestamp formatting for comparison later
    timestamp_format = sqlite_archiver_success.get_archive_timestamp_format()

    # trigger an archive rollover
    sqlite_archiver_success.manage_db_size(None, None)
    # determine the file was rolled over and a new one created with an appropriate name
    db_files = [os.path.join(db_dir, path) for path in os.listdir(db_dir)]
    assert len(db_files) == 2
    assert params.get("database") in db_files
    # make sure the timestamp on the newly created file is correct
    for file in db_files:
        if not file.endswith('historian_test.sqlite'):
            splits = file.rsplit(".", 2)
            timestamp = datetime.datetime.strftime(now, timestamp_format)
            assert timestamp == splits[1]

    # make sure the next update time has been updated
    assert datetime.datetime.strftime(sqlite_archiver_success.next_archive_time, timestamp_format) == \
           datetime.datetime.strftime(now + sqlite_archiver_success.archive_period, timestamp_format)
    # trigger another "rollover" - this one should not create any new files, the update time should not have passed
    sqlite_archiver_success.manage_db_size(None, None)
    # No new files should have been created or removed
    assert db_files == [os.path.join(db_dir, path) for path in os.listdir(db_dir)]


@pytest.mark.parametrize("archive_period,archive_period_units,err_message", [
    (-1, 'd', r"Archive period must be a positive whole number value"),
    (0, 'd', r"Archive period must be a positive whole number value"),
    (1, 'fail', re.escape("Archive period units must be m (months), d (days), w (weeks), h (hours), M (minutes)")),
    (1, 'week', re.escape(r"Archive period units must be m (months), d (days), w (weeks), h (hours), M (minutes)"))
])
def test_create_sqlite_archive_functs_failure(archive_period, archive_period_units, err_message):
    with pytest.raises(ValueError, match=err_message):
        SQLiteArchiverFuncts(params,
                             default_table_def,
                             archive_period=archive_period,
                             archive_period_units=archive_period_units)

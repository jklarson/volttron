import sqlite3
import datetime
import time

# For building a test DB to copy over

# connect to the sqlite3 db copied from sqlite historian
historian_conn = sqlite3.connect("historian_test.sqlite")
historian_cursor = historian_conn.cursor()

# get the table definition so we can build the table in the copied DB
create_table_query = "select sql from sqlite_master where name = 'data'"
historian_cursor.execute(create_table_query)
create_table = historian_cursor.fetchone()[0]

# get some data we can spam into the test db
copy_data_query = "with query_topics as (select topic_id from topics limit 10) select * from data where topic_id in " \
                  "query_topics limit 100;"
historian_cursor.execute(copy_data_query)
historian_data = historian_cursor.fetchall()

copy_conn = sqlite3.connect("historian_copy.sqlite")
copy_cursor = copy_conn.cursor()
copy_cursor.execute(create_table)
copy_conn.commit()

num_per_insert = len(historian_data)
num_inserted = 0

next_insert = historian_data

# run bulk inserts for the data queued up for insert
while num_inserted < 5000000:
    copy_cursor.execute('BEGIN TRANSACTION')
    copy_cursor.executemany('INSERT INTO data (ts, topic_id, value_string) values(?, ?, ?)', next_insert)
    copy_cursor.execute('COMMIT')
    num_inserted += num_per_insert

    # update the data so we don't have timestamp collisions
    next_insert = []
    for record in historian_data:
        now_string = datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%f%z")
        next_insert.append((now_string, record[1], record[2]))

count = 'select count(*) from data'
copy_cursor.execute(count)
print(copy_cursor.fetchone())

copy_conn.close()
historian_conn.close()

start_time = time.perf_counter()

# connect to the sqlite3 db copied from sqlite historian
copy_conn = sqlite3.connect("historian_copy.sqlite")
copy_cursor = copy_conn.cursor()

# get the table definition so we can build the table in the copied DB
create_table_query = "select sql from sqlite_master where name = 'data'"
copy_cursor.execute(create_table_query)
create_table = copy_cursor.fetchone()[0]

# Create the table in the rollover db
rollover_conn = sqlite3.connect("copy_rollover.sqlite")
rollover_cursor = rollover_conn.cursor()
rollover_cursor.execute(create_table)
rollover_conn.commit()

# find the current min and max row id's
min_row_id = 'select min(rowid) from data'
max_row_id = 'select max(rowid) from data'

copy_cursor.execute(min_row_id)
min_id = copy_cursor.fetchone()[0]

copy_cursor.execute(max_row_id)
max_id = copy_cursor.fetchone()[0]

# load the rollover table in chunks of 1 million records
copy_range = list(range(min_id, max_id, 1000000))

chunk_query = 'select * from data where rowid between ? and ?'

range_ind = 0
while range_ind + 1 <= len(copy_range):
    copy_cursor.execute(chunk_query, (copy_range[range_ind], min(copy_range[range_ind+1]-1, 5000000)))
    chunks = copy_cursor.fetchall()
    range_ind += 1

    rollover_cursor.execute('BEGIN TRANSACTION')
    rollover_cursor.executemany('INSERT INTO data (ts, topic_id, value_string) values(?, ?, ?)', chunks)
    rollover_cursor.execute('COMMIT')

# Delete the copied data from the original table
drop_query = 'delete from data where rowid between ? and ?'

range_ind = 0
while range_ind + 1 <= len(copy_range):
    copy_cursor.execute(drop_query, (copy_range[range_ind], min(copy_range[range_ind+1]-1, 5000000)))
    chunks = copy_conn.commit()
    range_ind += 1

# TODO Get record count statistics for old and new tables

count = 'select count(*) from data'
copy_cursor.execute(count)
print(f"count of data from historian db: {copy_cursor.fetchone()}")

count = 'select count(*) from data'
rollover_cursor.execute(count)
print(f"count of data from rollover db: {rollover_cursor.fetchone()}")

copy_conn.close()
rollover_conn.close()

time_diff = time.perf_counter() - start_time
print(f"Transfer complete in {time_diff} seconds")

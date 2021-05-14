#!/usr/bin/env python

"""Reports the cumulative data acquired by Synda, in a .csv file, for later plotting."""
"""Here is how to get ready to run this.  It involves making a temporary database with a "size"
column for datasets.  Normally only files have a "size".
For performance reasons, I indexed last_done_transfer_date.
  cp /p/css03/painter/db/sdt.db_6_2021-05-10 ~/db/sdt-tmp.db 
  sqlite3 ~/db/sdt-tmp.db
  # (Don't run the next line unless you are very sure that you're doing it to sdt-tmp.db ! )
  sqlite> DELETE FROM dataset WHERE status NOT LIKE 'complete%' AND status NOT LIKE 'published%';
  sqlite> ALTER TABLE dataset ADD size INT;
  sqlite> UPDATE dataset SET size=(SELECT SUM(size) FROM file WHERE file.dataset_id=dataset.dataset_id)
  sqlite> CREATE INDEX idx_dataset_6 on dataset (last_done_transfer_date);
  sqlite> .quit
"""


import sys, pdb
import logging
import sqlite3
import datetime
import debug
global db, conn, rcdf, rcd

db = '/home/painter/db/sdt-tmp.db'
rcdf = '/home/painter/db/rcd.csv'
conn = None
activities = [
 'all', 'AerChemMIP', 'C4MIP', 'CDRMIP', 'CFMIP', 'CMIP', 'DAMIP', 'DCPP', 'FAFMIP', 'GMMIP',
 'GeoMIP', 'HighResMIP', 'ISMIP6', 'LS3MIP', 'LUMIP', 'OMIP', 'PAMIP', 'PMIP', 'RFMIP',
 'ScenarioMIP', 'VolMIP' ]

def setup():
    """Initializes logging and the connection to the database, etc."""
    global db, conn, rcdf, rcd

    logfile = '/p/css03/scratch/logs/report_cumulative_data.log'
    logging.basicConfig( filename=logfile, level=logging.INFO, format='%(asctime)s %(message)s' )

    # normal:
    if conn is None:
        timeout = 12000  # in seconds; i.e. 200 minutes
        conn = sqlite3.connect( db, timeout )

    rcd = open( rcdf, 'w' )

def finish():
    """Closes connections to databases, etc."""
    global db, conn, rcdf, rcd
    conn.commit()
    conn.close()
    conn = None
    rcd.close()

setup()

rcd.write( "date,data_footprint\n" )
end_date = datetime.date(2018, 7, 1)
stop_date = datetime.date(2021, 5, 10)
#testing stop_date = datetime.date(2018, 7, 2)
delta = datetime.timedelta(days=1)
curs = conn.cursor()
while end_date <= stop_date:
    cmd = "SELECT SUM(size) FROM dataset WHERE last_done_transfer_date<'%s' "\
          % str(end_date)
    try:
        curs.execute( cmd )
        results = curs.fetchall()
    except Exception as e:
        logging.debug("report_cumulative_data saw an exception %s" %e)
        curs.close()
        raise e
    assert( len(results)==1 )
    size = results[0][0]
    rcd.write( str(end_date)[:10]+" 00:00:00,"+str(size)+".0\n" )
    end_date += delta
curs.close()

finish()

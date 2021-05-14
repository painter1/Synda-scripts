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
global db, conn, curs

db = '/home/painter/db/sdt-tmp.db'
rcdf = '/home/painter/db/rcd-'
conn = None
activities = [
 'all', 'AerChemMIP', 'C4MIP', 'CDRMIP', 'CFMIP', 'CMIP', 'DAMIP', 'DCPP', 'FAFMIP', 'GMMIP',
 'GeoMIP', 'HighResMIP', 'ISMIP6', 'LS3MIP', 'LUMIP', 'OMIP', 'PAMIP', 'PMIP', 'RFMIP',
 'ScenarioMIP', 'VolMIP' ]

def setup():
    """Initializes logging and the connection to the database, etc."""
    global db, conn, curs

    logfile = '/p/css03/scratch/logs/report_cumulative_data.log'
    logging.basicConfig( filename=logfile, level=logging.INFO, format='%(asctime)s %(message)s' )

    # normal:
    if conn is None:
        timeout = 12000  # in seconds; i.e. 200 minutes
        conn = sqlite3.connect( db, timeout )
    curs = conn.cursor()

def finish():
    """Closes connections to databases, etc."""
    global db, conn, curs
    conn.commit()
    conn.close()
    conn = None
    curs.close()

setup()

for activity in activities:
    rcd = open( rcdf+activity+'.csv', 'w' )
    rcd.write( "date,data_footprint\n" )
    end_date = datetime.date(2018, 7, 1)
    stop_date = datetime.date(2021, 5, 10)
    delta = datetime.timedelta(days=1)

    if activity=='all':
        cmd = "SELECT last_done_transfer_date,size,dataset_functional_id FROM dataset"
    else:
        cmd = "SELECT last_done_transfer_date,size,dataset_functional_id FROM dataset"+\
              " WHERE dataset_functional_id LIKE '%s%s%s'" % ( "%.",activity,".%" )
    curs.execute( cmd )
    results = curs.fetchall()  # date, size, dataset_functional_id
    # ...it's all strings, e.g. results[0]=
    # (u'2018-03-20 09:46:30.346505', 9940604,
    #  u'CMIP6.CMIP.IPSL.IPSL-CM6A-LR.piControl.r1i1p1f1.AERmon.od550lt1aer.gr.v20180314')
    results.sort( key=(lambda x: x[0] ) )
    lastsize = 0
    lastdate = "2000-01-01"
    for result in results:
        thissize = result[1] + lastsize
        thisdate = result[0][:10]
        if thisdate>lastdate:
            rcd.write( lastdate+" 00:00:00,"+str(lastsize)+".0\n" )
        lastsize = thissize
        lastdate = thisdate
    rcd.write( lastdate+" 00:00:00,"+str(lastsize)+".0\n" )
    rcd.close()

finish()

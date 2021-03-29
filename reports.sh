#!/bin/bash

# Daily reports.  This reports the present queue, performance since
# yesterday, and the results of the installation job begun yesterday

export LOGFILE=/var/log/synda/reports.log

export DATE=`date +%Y-%m-%dT%H:%M:%SZ`
echo >> $LOGFILE 2>&1
echo $DATE >> $LOGFILE 2>&1

echo "synda queue" >> $LOGFILE
synda queue >> $LOGFILE 2>&1

# PERF_START_DATE should be the previous end date; but if we run this
# daily at the same time every day, this is 24 hours ending now:
export PERF_START_DATE=`date --date=yesterday '+%Y-%m-%dT%H:%M'`
export PERF_END_DATE=`date '+%Y-%m-%dT%H:%M'`
# daily at the same time every day, this is midnight to midnight:
#export PERF_START_DATE=`date --date=yesterday '+%Y-%m-%dT00:00'`
#export PERF_END_DATE=`date '+%Y-%m-%dT00:00'`
export PERF="/home/painter/scripts/synda-perf.py $PERF_START_DATE $PERF_END_DATE"
echo >> $LOGFILE
echo $PERF >> $LOGFILE
$PERF >> $LOGFILE 2>&1

echo >> $LOGFILE
echo Installation summary: >> $LOGFILE 2>&1
export INSTALLFILE=/var/log/synda/install/install-`date  --date=yesterday --iso-8601=date`.log
/home/painter/scripts/count_installed.py $INSTALLFILE >> $LOGFILE 2>&1

echo >> $LOGFILE
echo waiting file counts by data_node: >> $LOGFILE 2>&1
sqlite3 -separator ' | ' /var/lib/synda/sdt/sdt.db "SELECT data_node,COUNT(*) FROM file WHERE status='waiting' GROUP BY data_node" >> $LOGFILE 2>&1
echo >> $LOGFILE
echo error file counts by data_node: >> $LOGFILE 2>&1
sqlite3 -separator ' | ' /var/lib/synda/sdt/sdt.db "SELECT data_node,COUNT(*) FROM file WHERE status='error' GROUP BY data_node" >> $LOGFILE 2>&1

echo >> $LOGFILE
echo transfer.log: >> $LOGFILE 2>&1
/home/painter/scripts/reports.py $PERF_START_DATE >> $LOGFILE 2>&1

echo >> $LOGFILE
echo last mark_published errors: >> $LOGFILE 2>&1
grep "Exception caught in mark_published_all" /p/css03/scratch/logs/mark_published.log | tail -3 >> $LOGFILE 2>&1

echo >> $LOGFILE
echo last daemon starts: >> $LOGFILE 2>&1
tail -3 /var/log/synda/daemon/daemon_start.log >> $LOGFILE 2>&1

#!/usr/bin/env python

"""Checks the error_history field of each file for whether it records file-specific errors which had
been repeated over an extended period of time.  Such files' statuses will be changed will be changed
from 'error' to 'error-badurl' or 'error-checksum'"""

import sys, pdb
import argparse, logging
import sqlite3
import debug
from dateutil.parser import parse
import datetime
global conn, curs

def setup( db='/var/lib/synda/sdt/sdt.db' ):
    """Initializes the connection to the database, etc."""
    # To test on a temporary copy of the database:
    #db = '/home/painter/db/sdt.db'
    global conn, curs
    conn = sqlite3.connect(db)
    curs = conn.cursor()

def finish():
    """Closes connections to databases, etc."""
    global conn, curs
    conn.commit()
    conn.close()

def confirm_yesnoquit():
    """Returns True if the user types "yes" or something similar, False for "no",
    or None for "quit"."""
    # Note that raw_input returns the empty string for "enter"
    yes = {'yes','y', 'ye', ''}
    no = {'no','n'}
    quits = {'quit','q'}

    choice = raw_input().lower()  # in Python 3, this is input().lower()
    if choice in yes:
        return True
    elif choice in no:
        return False
    elif choice in quits:
        return None
    else:
        sys.stdout.write("Please respond with 'yes', 'no', or 'quit'")

def tobe_permanent( error_history, error_in='ERROR 404', min_interval=5, min_errors=3 ):
    """If the error_history records repeated errors, that call for changing a file's status
    to a permanent error, return the new status.   Otherwise return None.
    error_history should be a list of 2-tuples (date,error).  The input error to check
    for should be supplied.  Presently (and probably forever) if must be one of
    "bad checksum" or "ERROR 404".
    The minimum interval between errors may be supplied, and defaults to 5.
    The minimum number of errors may be supplied, and defaults to 3."""
    if len(error_history)<min_errors:
        return None
    return_error = { 'ERROR 404':'error-badurl', 'bad checksum':'error-checksum' }
    assert error_in in return_error
    dates = [ e[0] for e in error_history if e[1]==error_in ]
    if len(dates)<min_errors:
        return None
    date_last_error = parse(dates[0])
    nerrors = 1
    dates.sort()
    for i in range(1,len(dates)):
        # Get the time since the last error.
        # Converting to totalseconds lets us support fractional days.
        interval = (parse(dates[i])- date_last_error).total_seconds()/3600./24
        if interval>=min_interval:
            # dates[i] is at least min_interval days after the previous error date.
            nerrors += 1
            date_last_error = parse(dates[i])
    if nerrors>=min_errors:
        return return_error[error_in]
    else:
        return None

def mark_permanent_errors( min_interval=5, nrepeats=3, dryrun=True, confirm=True ):
    """Check the database for files with 'error' status, whose error_history represents
    repeated errors, either 'ERROR 404' or 'bad checksum'.  For each such file, change its status
    to a permanent one (not affected by "synda retry"):  'error-badurl' or 'error-checksum'.
    The minimum interval between errors may be supplied, and defaults to 5.
    The minimum number of repeated errors may be supplied, and defaults to 3.
    """
    global conn, curs
    # At present, the shortest possible non-null error string is 45 characters:
    #  [('2020-06-08 15:17:13.121540', 'ERROR 404')]
    # The shortest possible one with two errors recorded is 90 characters:
    #  [('2020-06-09 10:49:35.255064', 'ERROR 404'), ('2020-06-09 13:47:21.039614', 'ERROR 404')]
    # So if we want three errors we only need look at strings with >=45*nrepeats characters.
    cmd = "SELECT file_id, filename, error_history FROM file WHERE " +\
          "status='error' AND error_history IS NOT NULL AND LENGTH(error_history)>=?"
    curs.execute( cmd, (45*nrepeats,) )
    results = curs.fetchall()
    for result in results:
        if result is None:
            break
        file_id = result[0]
        filename = result[1]
        error_history = eval(result[2])
        new_status = tobe_permanent(error_history,'ERROR 404',min_interval,nrepeats)
        if new_status is None:
            new_status = tobe_permanent(error_history,'bad checksum',min_interval,nrepeats)
        if new_status is not None:
            if dryrun:
                # print, don't log.  This is a debugging mode.
                print "file %s is ready for permanent error status as %s"%(filename,new_status)
                print "  error_history=%s"%error_history
            elif confirm:
                # Change the error status, but with user confirmation file-by-file
                # A filename may have multiple versions, but it's more understandable than file_id.
                print "change %s from status 'error' to '%s'?"%(filename,new_status)
                yesnoquit = confirm_yesnoquit()
                if yesnoquit==True:
                    cmd = "UPDATE file SET status=? WHERE file_id=?"
                    cmd_vars = ( new_status, file_id )
                    curs.execute( cmd, cmd_vars )
                    print "changed status to '%s'" % new_status
                    logging.info( "changed status of %s to '%s'" % (filename, new_status) )
                elif yesnoquit==False:
                    print "leaving status at 'error'"
                else:
                    print "leaving status at 'error' for this and subsequent files"
                    break
            else:
                # Change the error status, without asking for confirmation.
                # A filename may have multiple versions, but it's more understandable than file_id.
                cmd = "UPDATE file SET status=? WHERE file_id=?"
                cmd_vars = ( new_status, file_id )
                curs.execute( cmd, cmd_vars )
                logging.info( "changed status of %s to '%s'" % (filename, new_status) )

if __name__ == '__main__':
    # Set up logging and arguments, then call the appropriate 'run' function.
    logfile = '/p/css03/scratch/logs/permanent_error.log'
    logging.basicConfig( filename=logfile, level=logging.INFO, format='%(asctime)s %(message)s' )

    p = argparse.ArgumentParser(
        description="Convert repeated errors after multiple retries to permanent ones,"+
        " in some cases." )
    p.add_argument( "--interval", dest="interval", required=False, type=float, default=5, help=
                    "minimum interval between errors for both to be considered; in days." )
    p.add_argument( "--nrepeats", dest="nrepeats", required=False, type=int, default=3, help=
                    "number of repeated errors to change the error to a permanent one." )
    p.add_argument('--dryrun', dest='dryrun', action='store_true' )
    p.add_argument('--no-dryrun', dest='dryrun', action='store_false' )
    p.add_argument('--confirm', dest='confirm', action='store_true', default=False )
    p.add_argument('--no-confirm', dest='confirm', action='store_false', default=False )
    p.set_defaults( dryrun=False, confirm=False )

    args = p.parse_args( sys.argv[1:] )

    setup()
    logging.info( "started permanent_error_status, args=%s"%args )
    mark_permanent_errors( args.interval, args.nrepeats, dryrun=args.dryrun,
                           confirm=args.confirm )
    finish()


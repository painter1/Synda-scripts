#!/usr/bin/env python

"""Computes useful performance data from the Synda database.  The beginning and ending dates and
times should be provided in a modified ISO 8601 format without letter separators, e.g.
'2019-01-25 13:04'.  The third argument is a partial url, which is normally used to specify the
protocol and data node, e.g. gsiftp://vesg.ipsl.upmc.fr.  But the % wildcard is permitted, and a
longer url may be used to narrow the coverage further."""

import os, sys, glob
from pprint import pprint
import sqlite3
#import debug, pdb
import datetime
global conn, curs

def setup():
    """Initializes the connection to the database, etc."""
    global conn, curs
    # normal:
    conn = sqlite3.connect('/var/lib/synda/sdt/sdt.db')
    # test on a temporary copy of the database:
    #conn = sqlite3.connect('~/db/sdt.db')
    curs = conn.cursor()

def finish():
    """Closes connections to databases, etc."""
    global conn, curs
    conn.commit()
    conn.close()

def str2time( date ):
    """Given a date string such as '2019-01-25 13:04' or '2019-01-25 13:04:13.922788',
    this function returns a datetime object representing the date."""
    FMT_min = '%Y-%m-%d %H:%M'
    FMT_sec = '%Y-%m-%d %H:%M:%S'
    FMT_frac = '%Y-%m-%d %H:%M:%S.%f'
    try:
        return datetime.datetime.strptime( date, FMT_frac )
    except ValueError:
        try:
            return datetime.datetime.strptime( date, FMT_sec )
        except ValueError:
            return datetime.datetime.strptime( date, FMT_min )

def downloading_intervals( startin, stopin, file_intervals ):
    """Returns active_time: the amount of time, in seconds, within (start,stop) in which at least
    one of the files described by 'file_intervals' was being downloaded.
    Input parameters:
    - start and stop define the overal time interval.
    The times provided are strings suitable for str2time, e.g. "2019-01-25 13:04:00.123"
    - The list 'file_intervals' is a list of tuples from the database, of the form
    (start_date, end_date, <ignored>).  Each tuple defines a time interval in which one file was
    being downloaded.
    """
    start = str2time( startin )
    stop = str2time( stopin )
    file_ints = [ ( str2time(file_int[0]), str2time(file_int[1]) ) for file_int in file_intervals ]

    file_ints.sort( key=(lambda x: x[0]) )  # sort by each file's start_date.

    # Because file_ints is sorted, the following computes intervals in a sorted order, sorted by
    # the bottom (start) time.  Each file_int either extends an interval at the top, or starts a
    # new interval above the top of the previous interval.  That is, the intervals are disjoint,
    # and ordered by the top (stop) time as well as the bottom time.
    # intervals = [] # not used, but may be useful for debugging
    bot = file_ints[0][0]
    top = file_ints[0][1]
    active_time = 0
    for file_int in file_ints:
       if file_int[0]<=top:   # extend present interval
           top = max( top, file_int[1] )
       else:             # new interval, after all previous intervals and previous files' end_date.
           active_time += (top-bot).total_seconds()
           # intervals.append( (bot,top) )  # save the last interval (not used)
           bot = file_int[0]
           top = file_int[1]
    if top<stop:
        active_time += (top-bot).total_seconds()
    # intervals.append( (bot,top) )  # save the last interval (not used)

    return active_time

def url_hdr( url ):
    """url header, i.e. the protocol and data node but no more of the url."""
    upto_third_slash = url[: url.find('/', 2+url.find('//'))]
    return upto_third_slash

def url_hdrs( start, stop, server, method='aggregate' ):
    """Returns url headers (with protocol and data node) for transfers with times between
    'start' and 'stop', and a specified server.  These are the same transfers as for the
    corresponding call of perf_data()."""
    # If the SQL command is changed in perf_data, then this should be changed to match:
    cmd = ("SELECT url FROM file WHERE start_date>='{0}' AND " +\
           "end_date<='{1}' AND url LIKE '{2}%' AND " +\
           "status='done' AND size IS NOT NULL").format(start, stop, server)
    curs.execute( cmd )
    results = curs.fetchall()
    return list(set( [ url_hdr(r[0]) for r in results] ))

def perf_data( start, stop, server, method='aggregate' ):
    """Returns performance data for transfers with times between 'start' and 'stop', and a
    specified server.
    The times should be in a modified ISO 8601 format without letter separators, e.g.
    '2019-01-25 13:04'.  The server - both the data node and the protocol - is specified as the
    first characters of the url, e.g. 'gsiftp://esgf1.dkrz.de' or 'http://esgf1.dkrz.de'.
    Optionally you may provide a method argument to specify how the rate is to be computed."""
    cmd = ("SELECT start_date, end_date, size FROM file WHERE start_date>='{0}' AND " +\
           "end_date<='{1}' AND url LIKE '{2}%' AND " +\
           "(status='done' OR status='published') AND size IS NOT NULL").format(start, stop, server)
    # ...For more accuracy, I could include files overlapping the (start,stop) boundary, i.e.
    # end_date>{0} and start_date<{1}.  Then I would have to reduce the file size in proportion
    # to the amount of the file's download time which is within (start,stop).
    curs.execute( cmd )
    results = curs.fetchall()
    sizes =  [ size for (start_date,end_date,size) in results ]
    Nfiles = len(sizes)
    totsize = sum(sizes)
    if totsize==0:
        return None,None,None,None,None
    avgsize = totsize/Nfiles/1024./1024
    spf = 0 # don't want to compute it in non-default cases

    if method=='aggregate':  # (bytes downloaded)/(downloading time).  Takes parallelism
        #    into account, and doesn't count inactive time in (start,stop ).
        active_time = downloading_intervals( start, stop, results )
        if active_time>0:
            retrate = totsize/active_time/1024/1024. 
            retsize = totsize/1024/1024/1024.
        else:
            retrate = 0
            retsize = 0
        spf = active_time/len(sizes)
    elif method=='aggregate-crude':  # simply (bytes downloaded)/(stop-start).  Takes parallelism
        #    into account, but it's off, sometimes way off, if there are inactive periods.
        delta = str2time(stop) - str2time(start)
        retrate = totsize/delta.total_seconds()/1024/1024. 
        retsize = totsize/1024/1024/1024.
    elif method=='seqsize':  # size-weighted method, but based on separate rates for each file,
        # thus like "synda metric" except that the average is weighted by file size.
        # In other words, compute time as if everything were sequential.
        deltas = [ str2time(end_date)-str2time(start_date) for (start_date,end_date,size) in results ]
        delta = datetime.timedelta(0)   # sum() doesn't work on timedelta objects
        for dl in deltas:
            delta += dl
        retrate = totsize/delta.total_seconds()/1024/1024. 
        retsize = totsize/1024/1024/1024.
    elif method=='arith':  # simple arithmetic average
        rates = [ size/(str2time(end_date)-str2time(start_date)).total_seconds()
                  for (start_date,end_date,size) in results if size!=0 ]
        retrate = sum(rates)/1024/1024./len(rates)
        retsize = totsize/1024/1024/1024.
    else:  # the simple arithmetic average which Synda does, but still restricted to the
        #    protocol+server and the date range.  This is a bit less precise than arith because
        #    the 'rate' column in the database has been rounded to an integer.
        cmd = ("SELECT avg(rate) FROM file WHERE status='done' AND rate IS NOT NULL AND "+\
               "start_date>='{0}' AND end_date<='{1}' AND size IS NOT NULL AND "+\
               "url LIKE '{2}%'").format(start,stop,server)
        curs.execute( cmd )
        results = curs.fetchall()
        retrate = results[0][0]/1024/1024.
        retsize =  totsize/1024/1024/1024.
    return round(retrate,4), round(spf,4), round(retsize,4), round(avgsize,4), Nfiles

            
if __name__ == '__main__':
    setup()
    print "args=", sys.argv
    if len( sys.argv ) < 3:
        print "provide start time, stop time, and server in the form of"
        print " '2019-01-25 13:04' '2019-01-25 14:04' 'gsiftp://esgf1.umr-cnrm.fr'"
        print " You can use a % wildcard character when specifying the server."
        print " You can use a T instead of a space between the date and time."
    else:
        if False:  # for tests:
            for method in ['aggregate','aggregate-crude','seqsize','arith','synda']:
                rate,size= perf_data( sys.argv[1], sys.argv[2], sys.argv[3], method )
                if rate is None:
                    print "No data downloaded"
                else:
                    print method, '	',rate, "MiB/s", size, "GiB"
        else:
            # Times with a T work better in scripts, e.g. '2019-01-25T13:04'.
            # The Synda database uses a space between the date and time, e.g.
            # '2019-01-25 13:04'
            start = sys.argv[1].replace('T',' ')
            stop  = sys.argv[2].replace('T',' ')
            if len(sys.argv)>=4:
                server = sys.argv[3]
            else:
                server = '%'
            rate,spf,size,avgsize,Nfiles = perf_data( start, stop, server )
            if rate is None:
                print "No data downloaded"
            else:
                uhs = url_hdrs( start, stop, server )
                uhs.sort()
                print 'rate',rate, "MiB/s  Nfiles",Nfiles,"  size", size, "GiB", "avg size", avgsize, "MiB", uhs
                if len(uhs)>1:
                    for uh in uhs:
                        rate,spf,size,avgsize,Nfiles = perf_data( start, stop, uh )
                        print "rate {:6.2f}".format(rate),\
                            "MiB/s  Nfiles {:5d}".format(Nfiles),\
                            "  size {:8.2f}".format(size),\
                            "GiB", "  avg size {:8.2f}".format(avgsize), "MiB", uh

    finish()


    


#!/usr/bin/env python

# Python functions for generating reports pertaining to replication.

from datetime import datetime, timedelta
from pprint import pprint
import sys, os, pdb
import debug

global inst, scheme, TransferLOG, start_time
inst = {}
scheme = {}
start_time = 1 # an integer number of days before present, normally 1
TransferLOG  = '/var/log/synda/sdt/transfer.log'  # LLNL standard
DiscoveryLOG = '/var/log/synda/sdt/discovery.log' # LLNL standard
#TransferLOG  = '/etc/synda/sdt/log/transfer.log'  # master branch default
#DiscoveryLOG = '/etc/synda/sdt/log/discovery.log' # master branch default

def tail(f, n):
    """runs the operating system's 'tail', probably faster than
    doing it in Python"""
    stdin,stdout = os.popen2("tail -n "+str(n)+" "+f)
    stdin.close()
    lines = stdout.readlines(); stdout.close()
    return lines

def logsince( logfile, starttime, taillen=12345000 ):
    """returns lines of a log file since a start time.
    By default, we look at only the last 12,345,000 lines of the log file, as this is sufficient
    for all Synda use that I have seen.  A tenth of that is not sufficient.  A third of that
    works on a slow day.  Note that if start_time>1 this may not provide complete coverage.
    It is expected that each line will begin with a time.  The format of this time and of
    starttime format is Synda's, i.e.  "2020-10-22 11:32:12"."""
    starttime = starttime.replace('T',' ')
    lastlines = tail( logfile, taillen )
    datedlines = [ l for l in lastlines if l[0:4]==starttime[0:4] or
                   l[0:4]==str(int(starttime[0:4])+1) ]
    sincelines = [ l for l in datedlines if l[:19]>=starttime[:19] ]
    return sincelines

def logline_datanode( ll ):
    """Finds the first url (if any) in a line of transfer.log and returns the part before the
    second '//' (or ':2811//' for GridFTP), thus identifying its data_node.
    Returns this part of the url (scheme+domain), and also the schme (http or gsiftp) and a
    substring of the domain which identifies the institute - i.e., the last 2 or 3 parts of the
    domain). """
    global inst, scheme
    # first find the data node, e.g. http://vesg.ipsl.upmc.fr.
    i1 = ll.find('http')
    i2 = ll.find('gsiftp')
    i = min( len(ll) if i1<0 else i1, len(ll) if i2<0 else i2 )
    j = ll[i+9:].find('/')+9
    if ll[i:i+j].find(':2811')>0:
        j -= 5  # e.g. gsiftp://gridftp.ipsl.upmc.fr, not gsiftp://gridftp.ipsl.upmc.fr:2811
    datanode = ll[i:i+j]

    # Extract the scheme, i.e. http or gsiftp
    if datanode not in scheme:
        scheme[datanode] = datanode[:datanode.find('://')]

    # Extract enough to identify the institute, e.g. upmc.fr or fio.org.cn
    # Usually the upper two domain levels make it clear, but often the upper three are better.
    if datanode not in inst:
        doms = datanode.split('.')  # list of all domain levels
        if doms[-2:] == ['gc', 'ca']:
            inst[datanode] = 'CCCma'
        elif doms[-2:] == ['umr-cnrm', 'fr']:
            inst[datanode] = 'CNRM'
        elif doms[-2] in ['org','ac', 'edu', 'cma', 'noaa', 'apcc21', 'upmc', 'sigma2']:
            inst[datanode] = doms[-3:][0].upper()
        else:
            inst[datanode] = doms[-2:][0].upper()

    return datanode

def transfer_error_counts( sincelines ):
    """returns file transfer error counts in sincelines, a subset of transfer.log
    """
    known_errors = [
        "ERROR 503: Service Unavailable", "503 Service Temporarily Unavailable",
        "Server side credential failure", "Cannot find trusted CA certificate",
        "Valid credentials coult not be found",
        "/var/tmp/synda/sdt/1682/.esg/credentials.pem is not a valid file",
        "Temporary failure in name resolution", "unable to resolve host address",
        "Connection timed out", "Connection refused", "Connection reset by peer",
        "Bad Gateway" , "504 Gateway Time-out", "authorization failed",
        "Unable to establish SSL connection", "Connection closed at byte",
        "The GSI XIO driver failed to establish a secure connection.",
        "File corruption detected", "ERROR 404", "No such file or directory",
        "System error in open: Permission denied",
        "sdget_status=7", # i.e. sdget.sh was killed by SIGINT or SIGTERM.
        #                  This normally means that the daemon died.
        "No data received", "Operation not permitted",
        "Name or service not known", "ERROR 400: Bad Request",
        "globus_ftp_client: the operation was aborted"
    ]
    dn_errs = {}
    errdict = { e:0 for e in known_errors }
    known_errlines = []
    errlines = [ l for l in sincelines if l.find('SDDMDEFA-102 Transfer failed')>0 ]
    
    for err in known_errors:
        elines = [ l for l in errlines if l.find(err)>0 ]
        for ll in elines:
            datanode = logline_datanode(ll)
            if datanode not in dn_errs:
                dn_errs[datanode] = {}
            if err in dn_errs[datanode]:
                dn_errs[datanode][err] +=1
            else:
                dn_errs[datanode][err] = 1
        errdict[err] = len(elines)
        known_errlines += elines
    unknown_errlines = [ l for l in errlines if l not in known_errlines ]

    return len(known_errlines), len(unknown_errlines), errdict, dn_errs, unknown_errlines

def transfer_done_counts( sincelines ):
    """returns file transfer success ('done') counts in sincelines, a subset of transfer.log"
    """
    dn_done = {}
    # this duplicates the sincelines computation in transfer_error_counts, but performance isn't
    # critical and it simplifies coding...
    donelines = [ l for l in sincelines if l.find('Transfer done')>0 ]
    for ll in donelines:
        datanode = logline_datanode(ll)
        if datanode not in dn_done:
            dn_done[datanode] = 0
        dn_done[datanode] +=1
    return len(donelines), dn_done
    
def transfer_fallback_counts( sincelines ):
    """returns file transfer fallback (i.e. try another url) counts in sincelines, a subset of
    transfer.log
    """
    global inst, scheme
    dn_fallback = {}
    fb_dict = {}
    fallbacklines = [ l for l in sincelines if l.find('Url successfully switched')>0 ]
    for ll in fallbacklines:
        datanode1 = logline_datanode(ll[ll.find("old_url"):])
        datanode2 = logline_datanode(ll[ll.find("new_url"):])
        if datanode1 not in dn_fallback:
            dn_fallback[datanode1] = 0
        dn_fallback[datanode1] +=1
        if datanode1 not in fb_dict:
            fb_dict[datanode1] = {}
        fb_to = scheme[datanode2] if inst[datanode1]==inst[datanode2] else datanode2 
        if fb_to not in fb_dict[datanode1]:
            fb_dict[datanode1][fb_to] = 0
        fb_dict[datanode1][fb_to] +=1
        
    return len(fallbacklines), dn_fallback, fb_dict
    
def retraction_counts( starttime ):
    """Returns the retracted.py run summaries (normally just one) with numFound and Nchanges.
    Also returns those exceptions which retracted.one_query() catches from status_retracted.py.
    Usually these are "database is locked" exceptions which occurred despite multiple retries."""
    sincelines = logsince( '/p/css03/scratch/logs/retracted.log', starttime, taillen=12000 )
    # Normally we just want the last line.  But that won't work if there are two runs in a
    # single day, or a run hasn't finished yet.
    summaries = [l[l.find("End of retracted.py")+21:] for l in sincelines if
                 l.find("End of retracted.py.")>0]
    exceptions = [l[l] for l in sincelines if l.find("Failed with exception")>0]
    return summaries, exceptions

def interesting_transfer_error( line ):
    """Tests a transfer.log line for whether it calls for human attention."""
    # Omitted: ERROR log lines which would result in a download failure that is reported elsewhere.
    # Omitted: OpenID failures if continue_on_cert_errors=True.  If it's False, then SDDMDEFA-505
    # will be logged.  Also omitted is SYDLOGON-012 which merely duplicates another report, and
    # thus can only lead to confusion.
    if line[24:29]!='ERROR':
        return False
    elif line[30:75]=='SDDMDEFA-190 Download process has been killed':
        return False
    elif line[30:58]=='SDWATCHD-275 wget is stalled':
        return False
    elif line[30:51]=='SDDMDEFA-155 checksum':
        return False
    elif line[30:48]=='SDDMDEFA-002 size ':
        return False
    elif line[30:80]=='SDOPENID-200 Error occured while processing OpenID':
        return False
    elif line[30:84]=='SYDLOGON-800 Exception occured while processing openid':
        return False
    elif line[30:89]=='SDDMDEFA-502 Exception occured while retrieving certificate':
        return False
    elif line[30:73]=='SDDMDEFA-503   continue_on_cert_errors=True':
        return False
    elif line[30:61]=='SDDMDEFA-504 Ignoring exception':
        return False
    elif line[30:120]=='SYDLOGON-012 Error code=None,message=None while retrieving certificate from myproxy server':
        return False # This occurs iff an SDMYPROX error has been logged.
    return True

def interesting_transfer_errors( lines ):
    """Searches the supplied transfer.log lines for errors which call for human attention,
    and returns those lines."""
    terrors = []
    for line in lines:
        if interesting_transfer_error( line ):
            terrors.append( line )
    return terrors

def interesting_discovery_errors( lines ):
    """Searches the supplied discovery.log lines for error-level logs,
    and returns those lines."""
    terrors = []
    for line in lines:
        if line[24:29]=='ERROR':
            terrors.append( line )
    return terrors


if __name__ == '__main__':
    start_time = (datetime.now()-timedelta(days=start_time)).strftime('%Y-%m-%d %H:%m')
    print "From",start_time,':'
    sincelines = logsince( TransferLOG, start_time )
    print "searching", len(sincelines), "lines"
    donecount, dn_done = transfer_done_counts(sincelines)
    knownerr, unknownerr, errdict, dn_errs, unknowns = transfer_error_counts(sincelines)
    fallbackcount, dn_fallback, fb_dict = transfer_fallback_counts(sincelines)
    
    print "no. done files = ", donecount
    print "no. error files =", knownerr+unknownerr
    print "no. fallback files = ", fallbackcount
    print "...done, error, and fallback file counts, broken down by data_node:"
    datanodes = list(set(dn_done.keys()) | set(dn_errs.keys()) | set(dn_fallback.keys()))
    datanodes.sort( key=(lambda dn: inst[dn] ) )
    for dn in datanodes:
        if dn in dn_errs.keys():
            errcount = sum(dn_errs[dn].values())
        else:
            errcount = 0
        print '{:6.6} {:25.25} {:6d} {:6d} {:6d}'.format(
            inst[dn], dn, dn_done.get(dn,0), errcount, dn_fallback.get(dn,0) )
#    pprint(dn_done)

    print "error counts by error type:"
    for err in errdict.keys():
        if errdict[err]>0:
            print '  ','{:30.28} {:6d}'.format(err,errdict[err])
    print  '  ','{:30.30} {:6d}'.format('unknown',unknownerr)
#    pprint( errdict )

    print "...same errors, but broken down by data_node:"
    # formerly this was "for dn in dn_errs.keys()", but I want the output sorted by datanodes...
    for dn in datanodes:
        if dn not in dn_errs.keys():
            continue
        errsum = sum(dn_errs[dn].values())
        print "{:6.6} {:.<40.40}{:.>6d}".format( inst[dn], dn, errsum )
#        print "{}{:6d}".format(dn,errsum)
#        print dn, ' ', sum(dn_errs[dn].values())
        for err in dn_errs[dn]:
            print '  ','{:30.30} {:5d}'.format(err,dn_errs[dn][err])
#            print '   ',err,dn_errs[dn][err]
    print "sample lines with unknown errors:"
    pprint( unknowns[0:4] )

    print "\nfallback counts by original url and destination:"
    for dn in datanodes:
        if dn not in fb_dict.keys():
            continue
        fbsum = sum(fb_dict[dn].values())
        print "{:6.6} {:.<40.40}{:.>6d}".format( inst[dn], dn, fbsum )
        for fb in fb_dict[dn]:
            print '  ','{:30.30} {:5d}'.format(fb,fb_dict[dn][fb])
#    pprint( fb_dict )

    print "\nretraction summary since %s:"%start_time
    ret_counts, exceptions = retraction_counts(start_time)
    for retc in ret_counts:
        # Note retc ends with a newline, print ends with another newline which we don't need.
        sys.stdout.write( retc )
    sys.stdout.flush()
    print " %s exceptions" % len(exceptions)

    print "\ntransfer errors:"
    terrors = interesting_transfer_errors( sincelines )
    if len(terrors)==0:
        print "None"
    else:
        for line in terrors:
            print line

    print "\ndiscovery errors:"
    disclines = logsince( DiscoveryLOG, start_time )
    print "searching", len(disclines), "lines"
    terrors = interesting_discovery_errors( disclines )
    if len(terrors)==0:
        print "None"
    else:
        for line in terrors:
            print line

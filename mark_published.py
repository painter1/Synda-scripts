#!/usr/bin/env python

"""Input is a file listing dataset names and a Synda database.  The datasets must be completely
published.  That is, every file of the dataset has been published.
This script will check that the dataset's status in the Synda database is 'complete' or
'staged'.  If so, its status will be changed to 'published'.
Similarly, the files in the dataset will be assigned status 'published' if they are already
'done' or 'staged'."""

import os, sys
import tarfile, argparse
from pprint import pprint
import sqlite3, debug
import logging
global conn, dryrun
from retrying import retry
import pdb

dryrun = False  # Don't set it to True here.  Instead use the --dryrun argument.
datasets_already_published = 0
datasets_not_published_not_marked = 0
datasets_marked_published = 0

def setup(db):
    """Initializes the connection to the database, etc."""
    global conn
    # normal:
    conn = sqlite3.connect(db)  # typical db: '/var/lib/synda/sdt/sdt.db'
    #                               or test db: '/home/painter/db/sdt.db'
    #curs = conn.cursor()
    # safer to get the cursor when needed, and close it quickly: doesn't lock out other processes

def finish():
    """Closes connections to databases, etc."""
    global conn
    conn.commit()
    conn.close()

def mapfile_dates_available( only_after='000000' ):
    """This function looks in /p/user_pub/publish-queue/CMIP6-map-tarballs/ for file names of the
    form "mapfiles-NNNNNN.tgz" where NNNNNN is a 6-digit date.  It returns a list of such dates.
    Optionally you can provide an argument only_after, which is a 6-digit date.  Then the list will
    include only dates after only_after, and mapfile_dates_available(only_after)[0] will be the
    first 6-digit date after only_after, if there is one."""
    files = os.listdir("/p/user_pub/publish-queue/CMIP6-map-tarballs/")
    files = [ f for f in files if ( f[0:9]=='mapfiles-' and f[15:]=='.tgz' ) ]
    dates = [ f[9:15] for f in files if f[9:15]>only_after ]
    # Exclude dates like '2-5-19', a format used only in older files:
    dates = [ d for d in dates if d.find('-')<0 ]
    dates.sort()
    return dates

def next_suffix():
    """Returns the next suffix, used to identify the input file mapfile_SUFFIX.tgz and the output
    file files_not_found_SUFFIX."""
    last_suffix_f = '/p/css03/scratch/publishing/CMIP6_last_suffix'
    with open(last_suffix_f,'r') as f:
        last_suffix = f.readline().strip()
    available_suffixes = mapfile_dates_available( last_suffix )
    if len(available_suffixes)>0:
        return available_suffixes[0]
    else:
        logging.info( "Nothing for next_suffix() to do, all mapfiles have already been read." )
        sys.exit()    

# The retry decorator is mainly for "database is locked", which happens often enough:
# The decorator causes a retry after any exception.  It defines exponential backoff, waiting
# 2^x * 1000 milliseconds between retries, a maximum of 120 seconds (2 minutes), and gives up
# after 900 seconds (15 minutes)
#@retry(wait_exponential_multiplier=1000, wait_exponential_max=120000, stop_max_delay=900000)
# 2021.03.29: Wait longer.  The 2021.02.29 retry profile was failing every day.
# Double the time between retries, maximum of 10 minutes, give up after 1 hour.
@retry(wait_exponential_multiplier=1000, wait_exponential_max=600000, stop_max_delay=3600000)
def mark_published_synda( dataset_functional_id, filenotfound ):
    """The specified dataset has its status changed to 'published' if its status is 'complete'
    or 'staged'. Its files become 'published' if they are 'done' or 'staged'.  If a file is
    not found in the file system, then a message will be written to the supplied open text
    file, filenotfound.
    """
    logging.info( "entering mark_published_synda for dataset %s" %dataset_functional_id )
    if dataset_functional_id is None:
        return
    try:
        num_found, latest_version = dataset_published( dataset_functional_id, filenotfound )
    except Exception as e:
        logging.error( "mark_published_synda caught an exception from dataset_published: %s" % e )
        raise(e)
    if num_found==0:
        return

    try:
        cmd = "SELECT dataset_id FROM dataset WHERE dataset_functional_id='%s'" % dataset_functional_id
        curs = conn.cursor()
        curs.execute(cmd)
        results = curs.fetchall()
    except Exception as e:
        logging.error( "Exception in mark_published_synda 1: %s" % e )
        raise(e)
    finally:
        curs.close()

    if len(results)!=1:
        logging.debug( "dataset %s found %s times"%(dataset_functional_id,len(results)) )
        return
    dataset_id = results[0][0]

    try:
        cmd = "SELECT file_functional_id FROM file WHERE dataset_id=%s" % dataset_id
        curs = conn.cursor()
        curs.execute(cmd)
        results = curs.fetchall()
    except Exception as e:
        logging.error( "Exception in mark_published_synda 2: %s" % e )
        raise(e)
    finally:
        curs.close()

    all_successful = True
    try:
        for file_functional_id_tuple in results:
            this_successful = file_published( file_functional_id_tuple[0], filenotfound, latest_version )
            all_successful = all_successful and this_successful
    except Exception as e:
        logging.error( "mark_published_synda caught an exception from file_published: %s" % e )
        raise(e)

    if not all_successful:
        # Print a blank line so that the files in filenotfound will be visibly grouped by dataset.
        filenotfound.write(dataset_functional_id+"\n")
        filenotfound.write("\n")

    return

def dataset_published( dataset_functional_id, filenotfound ):
    """the dataset portion of mark_published_synda()"""
    global conn, dryrun
    global datasets_already_published, datasets_not_published_not_marked, datasets_marked_published

    try:
        cmd = "SELECT status FROM dataset WHERE dataset_functional_id='%s'" % dataset_functional_id
        curs = conn.cursor()
        curs.execute( cmd )
        results = curs.fetchall()
    except Exception as e:
        logging.debug( "Exception in dataset_published 1: %s" %e )
        raise e
    finally:
        curs.close()

    if len(results)==0:
        logging.info( "no dataset found matching %s" % dataset_functional_id )
        return 0, None
    elif len(results)>1:
        msg = "%s datasets found matching %s" % (len(results),dataset_functional_id )
        logging.error( "Exception generated in dataset_published due to: "+msg )
        raise Exception( msg )

    status = results[0][0]
    #if dryrun:
    #    print "  old status for %s is '%s'" % (dataset_functional_id,status)
    if status=='published':
        datasets_already_published += 1
    if status not in [ 'complete', 'staged', 'published' ]:
        datasets_not_published_not_marked += 1
        filenotfound.write( "status of %s\t unchanged at '%s'\n" %\
                            (dataset_functional_id,status) )
        return len(results), None
    if status!='published':
        datasets_marked_published += 1
        if dryrun:
            logging.info( "  changing %s\t from '%s' to 'published'" % (dataset_functional_id,status) )
        else:
            try:
                cmd = "UPDATE dataset SET status='published' WHERE dataset_functional_id='%s'" %\
                      dataset_functional_id
                curs = conn.cursor()
                curs.execute( cmd )
                conn.commit()
            except Exception as e:
                logging.debug( "Exception in dataset_published 2: %s" %e )
                raise e
            finally:
                curs.close()

    # For files_published(), we'll need to know whether this is the latest version.
    try:
        cmd = "SELECT path_without_version,version FROM dataset WHERE dataset_functional_id='%s'" %\
              dataset_functional_id
        curs = conn.cursor()
        curs.execute( cmd )
        results = curs.fetchall()
    except Exception as e:
        logging.debug( "Exception in dataset_published 3: %s" %e )
        raise e
    finally:
        curs.close()

    path_without_version = results[0][0]
    version = results[0][1]
    try:
        cmd = "SELECT version FROM dataset WHERE path_without_version='%s'" %\
              path_without_version
        curs = conn.cursor()
        curs.execute( cmd )
        results = curs.fetchall()
    except Exception as e:
        logging.debug( "Exception in dataset_published 4: %s" %e )
        raise e
    finally:
        curs.close()

    versions = [r[0] for r in results]
    if version==max(versions):
        latest_version = True
    else:
        latest_version = False

    return len(results), latest_version

def file_published( file_functional_id, filenotfound, latest_version ):
    """the file portion of mark_published_synda.
    file_functional_id identifies the file; filenotfound is an open file to which we can write
    messages, e.g. about a file which is not where expected, latest_version is True iff the file
    belongs to the latest version of its dataset.
    Note that if the file is not the latest version, it might have been deleted after being
    published.  This will not trigger a "missing file" message.
    The file's database status is _not_ changed to reflect whether the file has been found.
    Usually returns True if the file was changed to 'published' status or didn't need to be changed.
    Returns False if there was a problem, e.g. file doesn't exist where expected.
    """
    global conn, dryrun

    cmd = "SELECT status,local_path FROM file WHERE file_functional_id='%s'" % file_functional_id
    try:
        curs = conn.cursor()
        curs.execute( cmd )
        results = curs.fetchall()
    except Exception as e:
        logging.debug( "Exception in file_published 1: %s" %e )
        raise e
    finally:
        curs.close()

    if len(results)==0:
        filenotfound.write( "             no file in database matching %s" % file_functional_id )
        return False
    elif len(results)>1:
        msg = "%s files in database matching %s" % (len(results),file_functional_id )
        logging.error( "Exception generated in file_published due to: "+msg )
        raise Exception(msg)

    status = results[0][0]
    if status in ['done,deletesoon','maybe'] or status[0:5]=='error':
        # Do nothing. (returning False would trigger some output.)
        return True
        
    #print "old status for %s is %s" % (file_functional_id,status)
    if status not in [ 'done', 'staged' ]:
        if status=='published':
            return True
        else:
            # Lots of ways to get here, e.g. retracted data.
            filenotfound.write( "             Unusual status %s for %s\n" %\
                                (status,file_functional_id) )
            return False

    # The database says file is 'done' or 'staged'.  It is part of a dataset which
    # is 'published'.  But make sure that the file is really in the right location to
    # have been published.  If not, print a message.
    if latest_version:
        local_path = results[0][1]
        full_path = '/p/css03/esgf_publish/'+local_path
        if not os.path.isfile(full_path):
            scratch_path = '/p/css03/scratch/'+local_path
            filenotfound.write( "Missing file, not at %s\n" % full_path )
            if os.path.isfile(scratch_path):
                filenotfound.write( "             It is at     %s\n" % scratch_path )
            else:
                filenotfound.write( "             not found elsewhere.\n" )
            return False

    # All is well, mark the file as published.
    if not dryrun:
        cmd = "UPDATE file SET status='published' WHERE file_functional_id='%s'" %\
              file_functional_id
        try:
            curs = conn.cursor()
            curs.execute( cmd )
            conn.commit()
        except Exception as e:
            logging.debug( "Exception in file_published 2: %s" %e )
            raise e
        finally:
            curs.close()
    #print "new status for %s is %s" % (file_functional_id,'published')
    return True

def dataset_namever2functional_id( namever ):
    """Input namever is a dataset name and version number such as
    "cmip5.output1.IPSL.IPSL-CM5A-LR.esmFixClim1.mon.land.Lmon.r1i1p1,v20120526"
    or a full path (beginning with /p/css03/esgf_publish) such as
    "/p/css03/esgf_publish/CMIP6/CMIP/IPSL/IPSL-CM6A-LR/abrupt-4xCO2/r10i1p1f1/CFmon/hur/gr"
    or
    "/p/css03/esgf_publish/CMIP6/CMIP/IPSL/IPSL-CM6A-LR/abrupt-4xCO2/r10i1p1f1/CFmon/hur/gr/v20180914"
    or a publication mapfile name such as
    "-rw-r----- ames4/climate   385 2019-02-25 05:10 CMIP6.ScenarioMIP.IPSL.IPSL-CM6A-LR.ssp585.r1i1p1f1.SImon.sitemptop.gn.v20190119.map"
    or a mapfile name but with a path prepending "CMIP6.", e.g.
    " -rw-r----- ames4/climate   397 2019-04-22 00:09 p/user_pub/CMIP6-maps-done/CMIP6.DCPP.IPSL.IPSL-CM6A-LR.dcppC-amv-Trop-pos.r22i1p1f1.Omon.detoc.gn.v20190110.map"
    Output is a Synda dataset_functional_id such as
    "cmip5.output1.IPSL.IPSL-CM5A-LR.esmFixClim1.mon.land.Lmon.r1i1p1.v20120526"
    If the input is already a dataset_functional_id, then it is simply returned."""
    if namever.find('mapfile_run_')>0:
        # mapfile run log, not a dataset id
        return None
    if namever.find('.map')<=0:
        # not a publication mapfile; of no interest to us
        return None
    else:
        # name of a publication mapfile.  Get rid of everything before the file name, and
        # get rid of the final '.map'
        namever = namever[ namever.rfind(' ')+1 : namever.rfind('.map') ]
    if namever.find("p/user_pub/publish-queue/CMIP6-maps-done/")==0:
        namever = namever[41:]
    elif namever.find("p/user_pub/CMIP6-maps-done/")==0:
        namever = namever[27:]
    if namever.count('/')>7:
        # count('/')==8 is a path beginning with CMIP6/ and missing a version.
        # We'll assume it's a complete path as in this function's docstring; the final
        # version number is optional though.
        if namever[0:11]=='p/user_pub/':
            namever = '/'+namever
        if namever[-9]!='v' or not namever[-8:].isdigit():
            # We don't have a version number and need one.  Use the latest version.
            verdirs = [subd for subd in os.listdir(namever) if subd[0]=='v' and
                       subd[1:].isdigit()]
            version = max(verdirs)
            namever = os.path.join( namever, version )
        namever = os.path.relpath( namever, '/p/css03/esgf_publish')
    namever = namever.replace('/','.')

    # This is the place to add further checks or transformations as needed.
    # E.g. after splitting by ',' and '.', the last substring should start with 'v'.
    return namever.replace(',','.').strip()

def mark_published_all( listing_file, db, filenotfound_nom="files_not_found.txt" ):
    """The input identifies two files.
    The first file lists datasets which are published, one per line.
    The second file is a Synda database.
    In the database, each listed dataset will get status 'published' if it already has
    status 'complete' or 'staged'.  Similarly, each file of the dataset will get status
    'published' if its status is 'done' or 'staged'."""
    global datasets_already_published, datasets_not_published_not_marked, datasets_marked_published
    try:
        setup(db)
        filenotfound = open( filenotfound_nom, 'a' )
        tf = tarfile.open( listing_file )
        files = tf.getmembers()
        for fmap in files:
            # fmap is a TarInfo object describing a .map file
            fp = fmap.path
            # typical fp = 'CMIP6.CMIP.NCAR.CESM2.historical.r2i1p1f1.Emon.cSoil.gn.v20190308.map'
            functional_id = dataset_namever2functional_id( fp.strip() )
            # ... e.g. CMIP6.CMIP.NCAR.CESM2.historical.r2i1p1f1.Emon.cSoil.gn.v20190308
            if functional_id is None or functional_id.find('mapfile_run_')==0:
                # e.g. mapfile_run_1554479110.txt; it's just a mapfile run log
                continue
            try:
                mark_published_synda( functional_id, filenotfound )
            except Exception as e:
                logging.error( "mark_published_all caught an exception from mark_published_synda: %s" %e )
                raise(e)
    except Exception as e:
        logging.error( "Exception caught in mark_published_all: %s" % e )
        raise(e)
    finally:
        writeme = "number of datasets already published = %s" % datasets_already_published
        logging.info( writeme )
        filenotfound.write( writeme+'\n' )
        writeme = "number of datasets in list but shouldn't be = %s" %\
                  datasets_not_published_not_marked
        logging.info( writeme )
        filenotfound.write( writeme+'\n' )
        writeme = "number of datasets just marked as published = %s" %\
                  datasets_marked_published
        logging.info( writeme )
        filenotfound.write( writeme+'\n' )
        filenotfound.close()
        finish()

if __name__ == '__main__':
    logfile = '/p/css03/scratch/logs/mark_published.log'
    # Set level to logging.DEBUG to get logs of all database exceptions.
    logging.basicConfig( filename=logfile, level=logging.INFO, format='%(asctime)s %(message)s' )
    logging.info( "starting" )

    p = argparse.ArgumentParser(
        description="Change the status of listed datasets (and their contents) to 'published'"+ \
        "in a Synda database" )

    # keyword arguments, all optional and useful only for testing:
    p.add_argument( "--dryrun", required=False, action="store_true" )
    p.add_argument( "--suffix", required=False,
                    help="suffix of mapfiles file in /p/user_pub/publish_queue/CMIP6-map-tarballs"+\
                    "e.g. 190418 is the suffix of mapfiles-190418.tgz",
                    default=None )
    p.add_argument( "--published_datasets", required=False,
                    help="file containing a list of published datasets",
                    default=None )
    p.add_argument( "--database", required=False, help="a Synda database",
                    default="/var/lib/synda/sdt/sdt.db" )
    p.add_argument( "--files_not_found", required=False, help="files not found will be listed here",
                    default=None )

    args = p.parse_args( sys.argv[1:] )

    if args.suffix is None:
        suffix = next_suffix()
    else:
        suffix = args.suffix
    logging.info( "suffix = %s" % suffix )

    if args.dryrun==True:
        dryrun = True
    logging.info( "dryrun = %s" % dryrun )
    #if not dryrun:
    #    print "Hey, I'm testing!  Run with --dryrun"
    #    sys.exit()

    published_datasets = args.published_datasets
    if published_datasets is None:
        published_datasets = "/p/user_pub/publish-queue/CMIP6-map-tarballs/mapfiles-" +\
                             suffix + ".tgz"

    files_not_found = args.files_not_found
    if files_not_found is None:
        files_not_found = "/p/css03/scratch/logs/files_not_found_"+suffix

    try:
        mark_published_all( published_datasets, args.database, files_not_found )
    except:
        # The exception should have been logged already; quit.  The file
        # last_suffix_f won't be updated, so the same mapfiles will be retried
        # the next time this script is run.
        sys.exit(1)

    if args.suffix is None:
        last_suffix_f = '/p/css03/scratch/publishing/CMIP6_last_suffix'
        if dryrun:
            logging.info( "Would write %s to %s" % (suffix,last_suffix_f) )
        else:
            with open(last_suffix_f,'w') as f:
                f.write( suffix )

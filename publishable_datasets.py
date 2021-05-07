#!/usr/bin/env python

"""This script writes a list of the latest publishable CMIP-6 data.  This is simply the
status='complete' datasets which became complete since the last time this script was run.
It is expected that this script will be run no more than once per day.
The list is written to a file, with a name like
   '/p/user_pub/publish-queue/CMIP6-list-todo/datasets_since_<date>_<time>'
The date & time of the last run is read from a file
    /p/css03/scratch/publishing/CMIP6_sincewhen
And it is written there (overwriting the previous date&time) upon successful completion of this script.
They are also logged in /p/css03/scratch/logs/publishable_datasets.log.
A list of failed datasets may be written to  /p/css03/scratch/logs/ as well.

Although normally this script is run with no arguments, it can be run with an 'ending' option to run
on a smaller time range. Or read a list of datasets (e.g. the list of failed datasets from a
previous run); in which case the ending time is not updated, of course.

Paths, group names, etc. are hard-wired.  This is *not* a general-purpose script!
"""

import os, sys, datetime, shutil, stat, argparse
import socket, pwd, grp
from pprint import pprint
import sqlite3
import logging
import pdb, debug
global conn, curs, dryrun, std_file_perms, std_dir_perms

dryrun = False
std_file_perms = stat.S_IRUSR | stat.S_IWUSR | stat.S_IRGRP | stat.S_IWGRP | stat.S_IROTH 
std_dir_perms = std_file_perms | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH

def setup(db):
    """Initializes the connection to the database, etc."""
    global conn, curs
    # normal:
    conn = sqlite3.connect(db)  # typical db: '/var/lib/synda/sdt/sdt.db'
    #                               or test db: '/home/painter/db/sdt.db'
    #curs = conn.cursor() now done at the time of curs.execute() ...
    #...safer to get the cursor when needed, and close it quickly: doesn't lock out other processes

def finish():
    """Closes connections to databases, etc."""
    global conn, curs
    conn.commit()
    conn.close()

def chgrp_perms( path, group='climatew', permissions=None ):
    """Changes the group and permissions of the path; group as specified, and permissions as
    specified, defaulting to std_file_perms or std_dir_perms."""
    global std_file_perms, std_dir_perms
    if group is None:
        _group = -1 # means don't change the group
    elif not isinstance(group, int):
        _group = grp.getgrnam(group)[2]
    if permissions is None:
        if os.path.isdir(path):
            permissions = std_file_perms
        else:
            permissions = std_dir_perms
    os.chown( path, -1, _group )
    os.chmod( path, permissions )

def chgrp_perms_updown( path, group='climatew' ):
    """Changes the group of the path, its parents (below /css03/esgf_publish or equivalent) and
    children.  Also changes their permissions to std_file_perms or std_dir_perms."""
    global std_file_perms, std_dir_perms
    # First go down
    for (root,dirs,files) in os.walk(path):
        chgrp_perms( root, group, std_dir_perms )
        for file in files:
            chgrp_perms( os.path.join(root,file), group, std_file_perms )
    dir = path
    try:
        while len(dir)>21:   # len("/p/css03/esgf_publish")
            chgrp_perms( dir, group, std_dir_perms )
            dir = os.path.split(dir)[0]
    except:  # If we don't have write permission, we're finished.
        pass

def run_by_date( ending ):
    global conn, curs, dryrun, std_file_perms, std_dir_perms
    """This function extracts a list of publishable but unpublished datasets from the database,
    formats it, and sends it on to move_and_record() to move the data to esgf_publish and record
    this action in file lists."""
    # If we also want to do CMIP-5 data, the filenames will have to depend on the host name - not done yet.
    if socket.gethostname()!='aimsdtn6':
        raise Exception("implemented only for CMIP-6 on aimsdtn6")

    # Start database, beginning & ending times.
    db = '/var/lib/synda/sdt/sdt.db'
    setup( db )
    sincewhen = '/p/css03/scratch/publishing/CMIP6_sincewhen'
    with open(sincewhen,'r') as f:
        beginning = f.readline().strip()
    logging.info( "beginning=%s, ending=%s" % (beginning, ending) )
    if dryrun:
        print "beginning=%s, ending=%s" % (beginning, ending)

    # Get the new complete datasets from the database, and move them
    try:
        cmd = "SELECT path_without_version,version FROM dataset WHERE status='complete' AND " +\
              "latest_date>'%s' AND latest_date<='%s'" % (beginning, ending) 
        curs = conn.cursor()
        curs.execute( cmd )
        conn.commit()
        results = curs.fetchall()
        curs.close()
    except Exception as e:
        logging.warning( "database query failed, exception was %s" % e )
        logging.warning( "   query was %s" % cmd )
        finish()
        sys.exit()

    three_paths = [
        # scratch+version, +version-headers, esgf_publish+version
        ( os.path.join('/p/css03/scratch',r[0],r[1]), # /p/css03/scratch/CMIP6/activity/.../var/grid/version
          os.path.join(r[0],r[1]),                    #                  CMIP6/activity/.../var/grid/version
          os.path.join('/p/css03/esgf_publish',r[0],r[1])
          #                                        /p/css03/esgf_publish/CMIP6/activity/.../var/grid/version
      )
        for r in results ]

    finish()
    suffix = beginning.replace(' ','_') # ' ' was needed for database access, _ is better here
    move_and_record( three_paths, suffix )

    # If we got this far, it must have worked, except for listed datasets.
    # Write out the new beginning for the next run.
    # If we didn't get here, the next time-based run will use the old beginning and recompute
    # whatever was missed this time.
    if dryrun:
        print "Normally would write",ending,"to",sincewhen
    else:
        with open(sincewhen,'w') as f:
            f.write( ending )

def run_by_list( dataset_file ):
    """Input is the name of a text file listing datasets.  This sets up input for move_and_record,
    then calls it to move each dataset from /scratch/ to /esgf_publish/.  The datasets in the input
    file should be specified as paths, without headers but ending with the version directory, i.e.
    "CMIP6/activity/.../var/grid/version"    """
    three_paths = []
    # From one input path, build the three paths to send on to move_and_record:
    with open( dataset_file, 'r' ) as f:
        for line in f:
            vnh = line.strip()
            three_paths.append( ( os.path.join('/p/css03/scratch/',vnh),
                                 vnh,
                                 os.path.join('/p/css03/esgf_publish/',vnh) ) )

    # Build the filename suffix:
    if dataset_file[:16]=='unmoved_datasets':
        suffix = dataset_file[17:] # the identifying date, e.g. 2019-06-10 12:52:21
    else:
        suffix = ''
    time = str(datetime.datetime.now())
    suffix = '_'.join([suffix,time])
    suffix = suffix.replace(' ','_')

    # Do the real work:
    move_and_record( three_paths, suffix )


def move_and_record( three_paths, suffix ):
    """Move datasets from /scratch/ to /esgf_publish/.  Record successes in a file
    datasets_since_<suffix> and failures in a file unmoved_datasets_<suffix>.
    The first input is a list of 4-tuples, each identifying the dataset in a different way.
    A tuple is (scrv, vnh, epbv ) where:
      scrv is a full path in scratch ending in the version, like
                                                /p/css03/scratch/CMIP6/activity/.../var/grid/version
      vnh  is scrv without header directories,                   CMIP6/activity/.../var/grid/version
      epbv is scrv moved to /esgf_publish  /p/css03/esgf_publish/CMIP6/activity/.../var/grid/version
    """
    global conn, curs, dryrun, std_file_perms, std_dir_perms
    moved_datasets = []
    split_datasets = []
    nosrc_datasets = []
    failed_datasets = []
    if dryrun:
        for scrv,vnh,epbv in three_paths:
            print "would move",scrv,"to",epbv
    else:
        for scrv,vnh,epbv in three_paths:
            try:
                os.renames(scrv,epbv)
                chgrp_perms_updown( epbv, group='climatew' )
                logging.info( "moved %s to %s" % (scrv,epbv) )
                moved_datasets.append( (vnh,epbv) )
            except Exception as e:
                logging.warning( "could not move %s to %s due to %s" % (scrv,epbv,e) )
                if (os.path.isdir(epbv) and len(os.listdir(epbv))>0 and (
                        not os.path.isdir(scrv)) or len(os.listdir(scrv))==0):
                    # data have already been moved; nothing left here
                    moved_datasets.append( (vnh,epbv) )
                    logging.info("data is already in %s" % epbv)
                elif os.path.isdir(epbv) and len(os.listdir(epbv))>0 and\
                     os.path.isdir(scrv) and len(os.listdir(scrv))>0:
                    # data in both directories.  Probably the dataset was changed
                    split_datasets.append( vnh )
                    logging.info("data is in both %s and %s" % (scrv,epbv) )
                elif not os.path.isdir(scrv):
                    # The source doesn't exist, and it hasn't already been (substantively) moved.
                    nosrc_datasets.append( vnh )
                    logging.info("source %s does not exist" % scrv )
                else:
                    # don't know what's wrong, could be a permissions problem for making epbv
                    failed_datasets.append( vnh )
                    logging.info("unknown problem with moving data %s" %vnh )

    # Write a file listing the new locations of the new complete datasets.
    #   To prevent premature processing of the file, it will be written to /tmp, permissions limited,
    #   and then moved.  Finally permissions will be relaxed.
    # Write another file listing any new complete datasets which could not be moved.  This duplicates
    # logging information, but is convenient input for a future second try.
    tmpfile = "/tmp/datasets_since_%s" % suffix
    outfile = "/p/user_pub/publish-queue/CMIP6-list-todo/"+os.path.basename(tmpfile)
    nosrcfile = "/p/css03/scratch/logs/nosrc_datasets_%s" % suffix
    splitfile = "/p/css03/scratch/logs/split_datasets_%s" % suffix
    failfile = "/p/css03/scratch/logs/unmoved_datasets_%s" % suffix
    with open( tmpfile, 'w' ) as f:
        for path in [ pp[1] for pp in moved_datasets ]:
            f.write( "%s\n" % path )
    # owner only can read/write tmpfile:
    chgrp_perms( tmpfile, group='painter', permissions=(stat.S_IRUSR | stat.S_IWUSR ))
    if not dryrun:
        shutil.copy( tmpfile, os.path.dirname(failfile) ) # moved files; for logging
        shutil.move( tmpfile, os.path.dirname(outfile) )  # moved files; for publishing
        chgrp_perms( outfile, group='climatew', permissions=std_file_perms )
    with open( splitfile, 'w' ) as f:
        for path in [ p for p in split_datasets ]:
            f.write( "%s\n" % path )
    with open( nosrcfile, 'w' ) as f:
        for path in [ p for p in nosrc_datasets ]:
            f.write( "%s\n" % path )
    with open( failfile, 'w' ) as f:
        for path in [ p for p in failed_datasets ]:
            f.write( "%s\n" % path )

if __name__ == '__main__':
    # Set up logging and arguments, then call the appropriate 'run' function.
    logfile = '/p/css03/scratch/logs/publishable_datasets.log'
    logging.basicConfig( filename=logfile, level=logging.INFO, format='%(asctime)s %(message)s' )
    p = argparse.ArgumentParser(
        description="Prepare for publishing by moving and listing publishable datasets." )
    p.add_argument( "--ending", dest="ending",
                    help="Last completion time of datasets, example '2019-06-10 12:52:21'." +
                    "  Default is now.  Note: beginning is always ending of the last run.",
                    required=False, default=str( datetime.datetime.now() ) )
    p.add_argument( "--datasets", dest="dataset_file",
                    help="Name or path of a file listing datasets to be moved.  " +
                    "Include the version but not the header directories.  Example:  " +
                    "CMIP6/ScenarioMIP/CCCma/CanESM5/ssp460/r3i1p1f1/Omon/hfds/gn/v20190429  " +
                    "If --datasets is supplied, the --ending argument will be ignored." +
                    "If --datasets is not supplied, datasets were chosen by date.",
                    required=False, default=None )
    p.add_argument( "--dryrun", action="store_true" )
    args = p.parse_args( sys.argv[1:] )
    if args.dryrun==True:
        dryrun = True
    if args.dataset_file is  None:
        run_by_date( ending=args.ending )
    else:
        run_by_list( dataset_file=args.dataset_file )

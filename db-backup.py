#!/usr/bin/env python

"""Backs up the Synda database from /var/lib/synda/sdt/sdt.db to /p/css03/painter/db/.
The backup file will be named so as to reveal the date and the machine it came from.
If it is the first of the month, the backup file will be made read-only."""

import sys, os, shutil, stat, grp
import socket, datetime, subprocess
import sqlite3
import sqlitebck
import pdb, debug

std_file_perms = stat.S_IRUSR | stat.S_IWUSR | stat.S_IRGRP | stat.S_IWGRP | stat.S_IROTH 
ro_file_perms = stat.S_IRUSR | stat.S_IRGRP | stat.S_IROTH 

hostname = socket.gethostname()
if len(hostname)==8 and hostname[0:7]=='aimsdtn':
    hostname = hostname[7]    # normally 5 or 6 for aimsdtn5 or aimsdtn6

date = str(datetime.datetime.now().date())  # e.g. '2019-07-19'

tf = '_'.join(['sdt.db',hostname,date])
source = '/var/lib/synda/sdt/sdt.db'
dest = '/p/css03/painter/db/' + tf

# shutil.copy2( source, dest )       # preserves mod time, etc.
# There isn't anything here to deal with rsync errors, but it will soon
# go away anyway...
#subprocess.call(['rsync', '-a', source, dest])

srccon = sqlite3.connect(source)
dstcon = sqlite3.connect(dest)
with dstcon:
    # Back up from source to dest, one page at a time.  Break the source file into
    # 100 pages and allow 0.25 seconds between them so that other processes have
    # some access to the database.
# When a recent Python 3.7 is available, this will be the best solution:
#    srccon.backup( dstcon, pages=100, sleep=0.25 )
# But for now this is it.  The pages and sleep options are documented, but they don't work for me:
    sqlitebck.copy( srccon, dstcon )
dstcon.close()
srccon.close()

groupn = grp.getgrnam('synda')[2]  # group number of 'synda', currently 20
os.chown( dest, -1, groupn )       # like "chgrp synda $dest"
if len(date)==10 and date[8:10]=='01':
    # On the first of the month, make it read-only because this is more of
    # an archival database.
    os.chmod( dest, ro_file_perms )
else:
    # For other dates, I expect to delete the backup from time to time.
    os.chmod( dest, std_file_perms )

# Make another copy so we can always use the same name for the latest backup.
# The permissions for this one should be standard (group-writable) even when
# the original version is read-only.
dest2 = '/p/css03/painter/db/sdt6.db'
shutil.copy2( dest, dest2 )
os.chmod( dest2, std_file_perms )

#!/usr/bin/env python

"""Adds up the number of files and amount of data in an installation log file."""

import debug

Kval = 1000  # Value of a K - it might be 1000 or 1024, I don't know what Synda uses.
Mval = Kval*Kval
Gval = Kval*Mval
Tval = Kval*Gval

import sys
from pprint import pprint

# The installation log file should be the first (and only) argument.
# For example:  infile = '/home/painter/install.2020.10.19.log'
# The installation script should create this file, append to it with every
# "synda install" command, and, near the end, run this script.

def bytecount_for_people(num):
    # from  https://stackoverflow.com/questions/579310/formatting-long-numbers-as-strings-in-python
    num = float('{:.3g}'.format(num))
    magnitude = 0
    while abs(num) >= 1000:
        magnitude += 1
        num /= 1000.0
    return '{}{}'.format('{:f}'.format(num).rstrip('0').rstrip('.'),
                         ['', ' KB', ' MB', ' GB', ' TB'][magnitude])
def run( infile ):
    with open(infile) as f:
        lines = f.readlines()
    szlines = [line for line in lines if line.find('Once downloaded,')==0]
    ctlines = [line for line in lines if line.find('will be added')>0]

    szs = [ l.replace('Once downloaded, ','') for l in szlines ]
    szs = [ l.replace(' of additional disk space will be used.\n','') for l in szs ]
    szsKB = [ l.replace(' KB','') for l in szs if l.find('KB')>0 ]
    szsKB += [ l.replace(' kB','') for l in szs if l.find('kB')>0 ]
    szKB = sum(map(eval,szsKB))
    szsMB = [ l.replace(' MB','') for l in szs if l.find('MB')>0 ]
    szMB = sum(map(eval,szsMB))
    szsGB = [ l.replace(' GB','') for l in szs if l.find('GB')>0 ]
    szGB = sum(map(eval,szsGB))
    szsTB = [ l.replace(' TB','') for l in szs if l.find('TB')>0 ]
    szTB = sum(map(eval,szsTB))
    assert( len(szs) == len(szsKB)+len(szsMB)+len(szsGB)+len(szsTB) )
    sz = Kval*szKB + Mval*szMB + Gval*szGB + Tval*szTB
    print "Total size installed =", bytecount_for_people(sz)

    cts = [ l.replace(' file(s) will be added to the download queue.\n', '') for l in ctlines ]
    ct = sum(map(eval,cts))
    print "Total number of files installed =", ('{:,}').format(ct)

if __name__ == '__main__':
    if len( sys.argv ) > 1:
        installlog = sys.argv[1]
        run( installlog )
    else:
        print "Supply the installation log file"

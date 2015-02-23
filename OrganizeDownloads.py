#!/usr/bin/python

# Divides the downloads into folders by num_downloads
# The downloads must have been done first

import sqlite3
import sys
import os

def organize(dbFile='apks.db', outFolder='apks'):
    if not os.path.isdir(outFolder):
        print 'Error: directory %s does not exist' % outFolder
    
    with sqlite3.connect(dbFile) as db:
        os.chdir(outFolder)
        cursor = db.cursor()
        for fileName in os.listdir('.'):
            if not os.path.isfile(fileName):
                continue
            apkName = fileName[:-4]
            cursor.execute('SELECT ndownload FROM apks WHERE name=?', [apkName])
            row = cursor.fetchone()
            ndownload = str(row[0])
            if not os.path.isdir(ndownload):
                os.makedirs(ndownload)
            os.rename(fileName, ndownload + '/' + fileName)

def usage():
    print "Usage: OrganizeDownloads <apks.db> <downloadFolder>"

if __name__ == '__main__':
    if len(sys.argv) != 3:
        usage()
        sys.exit(1)

    organize(sys.argv[1], sys.argv[2])

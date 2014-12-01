#!/usr/bin/python

# Downloads metadata and files from Play Drone Archives
# Currently supports:
#  - save - save metadata directly to current SQLite3 schema
#  - cache - save json metadata to very simple key value store PickleDB
#  - convert - take json metadata from PickleDB and store into SQLite3 schema

import os
import json
import shutil
import string
import hashlib
import logging
import sqlite3
import urllib2
import sys
from datetime import date, timedelta
from xml.etree import ElementTree
from bs4 import BeautifulSoup

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

url_base  = 'https://archive.org/download/'
date_pat = '%%%DATE%%%'
suffix_pat = '%%%SUFFIX%%%'
set_name_templ = '-'.join(['playdrone-metadata', date_pat, suffix_pat])

def GetSetWithDay(suffix, day):
    """
    Returns (set_name, response)
    response is the HTTP repsonse to the xml containing the list of files 
    for the suffix and day crawled specified in str format.
    set_name has suffix and day inserted
    Returns (None, _) if no such set is found.
    """
    set_name = string.replace(set_name_templ, suffix_pat, suffix)
    set_name = string.replace(set_name, date_pat, day)
    set_url  = 'https://archive.org/download/' + set_name
    set_xml  = set_url + '/' + set_name + '_files.xml'
    try:
        response = urllib2.urlopen(set_xml)
    except urllib2.URLError:
        response = None
        logger.debug('Set %s does not exist.' % set_xml)
    return set_name, response

def CheckSetValid(url):
    """
    Returns True or False
    Indicates whether or not the set at the url has sufficient data
    For now, we define "sufficient data" as more than 50 rows
    """
    html = urllib2.urlopen('https://archive.org' + url)
    soup = BeautifulSoup(html)
    if len(soup.find_all('td', 'ttl')) > 50:
        return True
    logger.debug('Skipped %s due to insufficient data' % url)
    return False

def List(suffix_range=range(256)):
    """
    Returns an iterator of (name of latest metadata set, name of apk) for
    for each suffix in @suffix_range.

    Archive.org stores multiple versions of each set, dated by the
    date that a version was crawled.  A set may not be crawled
    everyday. 
    """
    
    # Get total number of pages
    html = urllib2.urlopen('https://archive.org/search.php?query=collection%3Aplaydrone-metadata&sort=-publicdate')
    soup = BeautifulSoup(html)
    link = soup.find('a', text='Last').get('href')
    equal_sign = link.rfind('=')
    tot_pages = int(link[(equal_sign+1):])

    # First, store the most recent days for the suffix_range in a dict
    # Note that the url used already sorts results in most recent order of date
    days = {}
    count = 0
    page = 1
    for suffix in suffix_range:
        days[suffix] = False
    while count < len(suffix_range) and page <= tot_pages:
        html = urllib2.urlopen('https://archive.org/search.php?query=collection%3Aplaydrone-metadata&sort=-date&page=' + str(page))
        soup = BeautifulSoup(html)
        for link in soup.find_all('a', 'titleLink'):
            link_text = link.get('href')
            suffix = int(link_text[-2:],16)
            if days.get(suffix) == False and CheckSetValid(link_text):
                days[suffix] = link_text[28:-3]
                count += 1
                logger.debug('%d: Most recent metadata set for bucket %d is dated %s' % (count, suffix, days[suffix]))
                if count >= len(suffix_range):
                    break;
        page += 1

    # Generator        
    for suffix in suffix_range:
        set_name, response = GetSetWithDay(format(suffix, '02x'), days[suffix])
        logger.debug('List set %s' % set_name)
        for elem in ElementTree.parse(response).findall('file'):
            apk = elem.attrib['name']
            if not apk.endswith('.json'):
                continue
            apk = os.path.splitext(apk)[0]
            yield (set_name, apk)

VERSION=0
CATEGORY=1
NDOWNLOAD=2
def GetApkInfo(set_name, apk_name, include_response=False):
    """
    Returns the info as a tuple and the response as the json string dump received from the url (if prompted).
    """
    info_url = url_base + set_name + '/' + apk_name + '.json'
    try:
        response = urllib2.urlopen(info_url)
        info = json.load(response)
        version = info['details']['app_details']['version_code']
        # 'category' is a singleton list
        category  = info['details']['app_details']['app_category'][0]
        # need to strip the trailing '+' from 'num_downloads'
        ndownload = info['details']['app_details']['num_downloads'][:-1]
    except:
        logger.warning('Cannot get info %s' % info_url)
        if include_response:
            return None, None
        else:
            return None

    return_info = (version, category, int(ndownload.replace(',','')))
    if include_response:
        return json.dumps(info), return_info
    else:
        return return_info
    
def GetSetSuffix(apk_name):
    return hashlib.sha1(apk_name).hexdigest()[:2]

def DownloadApk(apk_name, version_code, filename=None):
    if filename is None:
        filename = apk_name + '.apk'
    suffix = GetSetSuffix(apk_name)
    apk_url = url_base  + 'playdrone-apk-%s/%s-%d.apk' % (suffix, apk_name, version_code)
    logger.debug('Download %s to %s' % (apk_url, filename))
    with open(filename, 'wb') as out:
        try:
            response = urllib2.urlopen(apk_url)
            shutil.copyfileobj(response, out)
        except urllib2.URLError:
            logger.warning('Cannot download %s to %s' % (apk_url, filename))

def Save(db_filename='apks.db', grep=lambda x:True):
    try:
        os.unlink(db_filename)
    except OSError:
        pass
    with sqlite3.connect(db_filename) as db:
        c = db.cursor()
        c.execute('''CREATE TABLE apks (
                     name TEXT PRIMARY KEY,
                     version INTEGER,
                     category TEXT,
                     ndownload INTEGER)''')
        for set_name, apk_name in List():
            info = GetApkInfo(set_name, apk_name)
            if info is None or not grep(info):
                logger.debug('Skipped %s' % apk_name)
                continue
            logger.debug('Insert %s,%s to %s' % (apk_name, info, db_filename))
            c.execute('insert or replace into apks values(?, ?, ?, ?)',
                      (apk_name,) + info)
            db.commit()

def Cache(db_filename='cache.db', grep=lambda x:True):
    try:
        os.unlink(db_filename)
    except OSError:
        pass
    with sqlite3.connect(db_filename) as db:
        c = db.cursor()
        c.execute('''CREATE TABLE apks (
                     name TEXT PRIMARY KEY,
                     json TEXT)''')
        for set_name, apk_name in List():
            response_str, info = GetApkInfo(set_name, apk_name, True)
            if info is None or not grep(info):
                logger.debug('Skipped %s' % apk_name)
                continue
            logger.debug('Inserted %s' % apk_name)
            c.execute('insert or replace into apks values(?, ?)',
                      (apk_name, response_str))
            db.commit()

def Usage():
    print "Usage: ApkArchives [save|cache|convert]"

if __name__ == '__main__':
    if len(sys.argv) != 2:
        Usage()
        sys.exit(1)
    
    if sys.argv[1] == 'save':
        Save(grep=lambda x: x[NDOWNLOAD] >= 10000)
    elif sys.argv[1] == 'cache':
        Cache(grep=lambda x: x[NDOWNLOAD] >= 10000)
    elif sys.argv[1] == 'convert':
        print "Not implemented yet"
    else:
        Usage()

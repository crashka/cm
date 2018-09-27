#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Reference data (refdata) module

Note that the term "refdata" is used to mean both individual data sources, as well as the
overall topic of reference (i.e. external) data for CMIR (we could use "refdatasrc" for
the former, but it's kind of cumbersome, so we'll deal with the ambiguity, and resolve
in context/situ)
"""

from __future__ import absolute_import, division, print_function

import os.path
import re
import json
import glob
import datetime as dt
from time import sleep
import logging

import click

import core
from utils import LOV, prettyprint, strtype, collecttype

#####################
# core/config stuff #
#####################

# shared resources from core
BASE_DIR     = core.BASE_DIR
cfg          = core.cfg
log          = core.log
sess         = core.sess
dflt_hand    = core.dflt_hand
dbg_hand     = core.dbg_hand
FETCH_INT    = core.FETCH_INT
FETCH_DELTA  = core.FETCH_DELTA

REFDATA_BASE = cfg.config('refdata_base')
REFDATA      = cfg.config('refdata')

##############################
# common constants/functions #
##############################

ConfigKey      = LOV(['URL_FMT',
                      'CATEGORIES',
                      'HTTP_HEADERS',
                      'FETCH_INTERVAL'], 'lower')
REQUIRED_ATTRS = set([ConfigKey.URL_FMT,
                      ConfigKey.CATEGORIES])

# the following correspond to hardwired RefData member variables
INFO_KEYS      = set(['name',
                      'status',
                      'config',
                      'state',
                      'categories'])

# local module LOVs
Status         = LOV(['NEW',
                      'ACTIVE',
                      'INVALID',
                      'DISABLED'], 'lower')
CatDataAttr    = LOV(['FILE',
                      'SIZE',
                      'STATUS',
                      'TIMESTAMP'], 'lower')
CatDataStatus  = LOV(['OK',
                      'NOTOK'], 'lower')

#################
# RefData class #
#################

class RefData(object):
    """Represents a refdata source defined in config.yml
    """
    @staticmethod
    def list():
        """List refdata sources that have been created

        :return: sorted list of refdata sources (same as directory names)
        """
        dirs = glob.glob(os.path.join(BASE_DIR, 'refdata', '*'))
        return sorted([os.path.basename(dir) for dir in dirs])

    def __init__(self, name):
        """Sets status field locally (but not written back to info file)
        """
        if name not in REFDATA:
            raise RuntimeError("RefData \"%s\" not known" % (name))
        log.debug("Instantiating RefData(%s)" % (name))
        self.name = name
        self.status = Status.NEW
        self.config = REFDATA_BASE.copy()
        self.config.update(REFDATA[name])
        for attr in REQUIRED_ATTRS:
            if attr not in self.config:
                raise RuntimeError("Required config attribute \"%s\" missing for \"%s\"" % (attr, name))

        # extract tokens in url_fmt upfront
        self.tokens = re.findall(r'(\<[A-Z_]+\>)', self.url_fmt)
        if not self.tokens:
            raise RuntimeError("No tokens in URL format string for \"%s\"" % (name))

        # these are the directories/files for the current refdata source
        self.refdata_dir       = os.path.join(BASE_DIR, 'refdata', self.name)
        self.refdata_info_file = os.path.join(self.refdata_dir, 'refdata_info.json')
        self.category_dirs     = {cat: os.path.join(self.refdata_dir, cat)
                                  for cat in self.config[ConfigKey.CATEGORIES].keys()}
        self.http_headers      = self.config.get(ConfigKey.HTTP_HEADERS)

        self.state = None
        self.categories = None
        self.load_state()
        if self.status == Status.NEW:
            self.status = Status.ACTIVE if self.valid() else Status.INVALID

        self.last_fetch = dt.datetime.utcnow() - FETCH_DELTA

    def __getattr__(self, key):
        try:
            return self.config[key]
        except KeyError:
            raise AttributeError()

    def refdata_info(self, keys = INFO_KEYS, exclude = None):
        """Return refdata info (canonical fields) as a dict comprehension
        """
        stat = str(self.status)
        if not collecttype(keys):
            keys = [keys]
        if collecttype(exclude):
            keys = set(keys) - set(exclude)
        return {k: v for k, v in self.__dict__.items() if k in keys}

    def store_state(self):
        """Writes refdata info (canonical fields) to refdata_info.json file
        """
        with open(self.refdata_info_file, 'w') as f:
            json.dump(self.refdata_info(), f, indent=2)
        log.debug("Storing state for %s\n%s" % (self.name, prettyprint(self.refdata_info(), noprint=True)))

    def load_state(self, force = False):
        """Loads refdata info (canonical fields) from refdata_info.json file
        """
        if self.state is None or force:
            self.state = {}
            self.categories = {}
            if os.path.isfile(self.refdata_info_file) and os.path.getsize(self.refdata_info_file) > 0:
                with open(self.refdata_info_file) as f:
                    refdata_info = json.load(f)
                self.state.update(refdata_info.get('state', {}))
                self.categories.update(refdata_info.get('categories', {}))
        log.debug("Loading state for %s\n%s" % (self.name, prettyprint(self.refdata_info(), noprint=True)))

    def build_url(self, cat, key):
        """Builds category data URL based on url_fmt, which is a required attribute in the refdata info

        :param cat: must be valid category name for refdata source
        :param key: typically represents starting initial
        :return: string
        """
        catattrs = self.config[ConfigKey.CATEGORIES][cat]
        # this is a magic variable name that matches a URL format token
        starts_with = key
        url = self.url_fmt
        for token in self.tokens:
            token_var = token[1:-1].lower()
            value = vars().get(token_var) or catattrs.get(token_var) or getattr(self, token_var, None)
            if not value:
                raise RuntimeError("Token attribute \"%s\" not found for \"%s\"" % (token_var, self.name))
            url = url.replace(token, str(value))

        return url

    def check(self, validate = False):
        """Return True if refdata exists (and passes validation test, if requested)
        """
        created = os.path.exists(self.refdata_dir) and os.path.isdir(self.refdata_dir)
        if not validate:
            return created
        else:
            return created and self.valid()

    def valid(self):
        """Return True if refdata is validated
        """
        # TEMP: for now, just check integrity of directory structure!!!
        valid = True
        for cat_dir in self.category_dirs.values():
            valid = valid and os.path.exists(cat_dir) and os.path.isdir(cat_dir)
        return valid

    def create(self, dryrun = False):
        """Create refdata (raise exception if it already exists)
        """
        if self.check():
            raise RuntimeError("RefData \"%s\" already exists" % (self.name))
        self.state = {}
        self.categories = {cat: {} for cat in self.category_dirs.keys()}
        if not dryrun:
            os.mkdir(self.refdata_dir)
            for cat_dir in self.category_dirs.values():
                os.mkdir(cat_dir)
            self.store_state()
        else:
            print(self.__dict__)

    def catdata_name(self, cat, key):
        """Category data segment name is cat (name) + key
        """
        return cat + ':' + key

    def catdata_file(self, cat, key):
        """Category data file is catdata name + data representation extension
        """
        filename = self.catdata_name(cat, key) + '.' + self.catdata_ext
        return os.path.join(self.category_dirs[cat], filename)

    def fetch_categories(self, cat, key = None, dryrun = False, force = False):
        """Retrieve specified category data and write to file system
        """
        # TEMP: for now, only fetch for explicit cat(s) and key(s)
        if not cat or not key:
            log.debug("Both category(ies) and key(s) must be specified to fetch for %s", (self.name))
            return  # nothing to do
        cats = [cat] if not collecttype(cat) else cat
        if strtype(key):
            m = re.match(r'([a-z])-([a-z])$', key.lower())
            if m and ord(m.group(1)) <= ord(m.group(2)):
                keys = [chr(charcode) for charcode in range(ord(m.group(1)), ord(m.group(2)) + 1)]
            else:
                keys = [key]
        else:
            keys = [key] if not collecttype(key) else key

        log.debug("Fetching refdata for categories %s and keys %s" % (cats, keys))
        for cat in cats:
            for key in keys:
                catdata_name = self.catdata_name(cat, key)
                catdata_file = self.catdata_file(cat, key)
                if os.path.exists(catdata_file) and os.path.getsize(catdata_file) > 0:
                    if not force:
                        log.info("Skipping fetch for \"%s\", file exists" % (catdata_name))
                        continue
                    else:
                        # STUPID to call getsize() again, but keeps things cleaner above!!!
                        log.info("Forcing overwrite of existing file (size %d) for \"%s\"" %
                                 (os.path.getsize(catdata_file), catdata_name))
                catdata_text = self.fetch_category(cat, key)
                log.debug("Content for category data \"%s\": %s..." % (catdata_name, catdata_text[:250]))
                if not dryrun:
                    with open(catdata_file, 'w') as f:
                        f.write(catdata_text)
                    status = CatDataStatus.OK
                    self.categories[cat][catdata_name] = {
                        CatDataAttr.FILE      : os.path.relpath(catdata_file, self.refdata_dir),
                        CatDataAttr.SIZE      : len(catdata_text),
                        CatDataAttr.STATUS    : status,
                        CatDataAttr.TIMESTAMP : self.last_fetch.isoformat() + ' UTC'
                    }
                    self.store_state()

    def fetch_category(self, cat, key):
        """Fetch single category data request (by key)
        """
        # TODO: create context manager for this HTTP throttling mechanism!!!
        elapsed = dt.datetime.utcnow() - self.last_fetch
        if elapsed < FETCH_DELTA:
            sleep_delta = FETCH_DELTA - elapsed
            sleep((sleep_delta).seconds + (sleep_delta).microseconds / 1000000.0)

        catdata_url = self.build_url(cat, key)
        log.debug("Fetching from %s (headers: %s)" % (catdata_url, self.http_headers))
        r = sess.get(catdata_url, headers=self.http_headers)
        catdata_content = r.content

        self.last_fetch = dt.datetime.utcnow()
        # TODO: this really needs to return metadata around the catdata, not
        # just the contents!!!
        return catdata_content


#####################
# command line tool #
#####################

@click.command()
@click.option('--list',      'cmd', flag_value='list', default=True, help="List all (or specified) refdata sources, and their categories")
@click.option('--create',    'cmd', flag_value='create', help="Create new refdata source (skip if refdata source exists)")
@click.option('--fetch',     'cmd', flag_value='fetch', help="Fetch data by category for refdata source")
@click.option('--cat',       help="Category (or comma-separated list of categories) to fetch")
@click.option('--key',       help="Category-dependent key/index for category data (e.g. first letter or name)")
@click.option('--force',     is_flag=True, help="Overwrite existing category data (otherwise skip over), applies only to --fetch")
@click.option('--dryrun',    is_flag=True, help="Do not execute writes/commits; log to INFO level instead")
@click.option('--debug',     default=0, help="Debug level")
@click.argument('name',      default='all', required=True)
def main(cmd, cat, key, force, dryrun, debug, name):
    """Manage reference data for comma-separated list of refdata sources (or 'all')
    """
    if debug > 0:
        log.setLevel(logging.DEBUG)
        log.addHandler(dbg_hand)

    if name == 'all':
        refdata_names = REFDATA.keys()
    else:
        refdata_names = name.lower().split(',')

    if cmd == 'list':
        for refdata_name in refdata_names:
            refdata = RefData(refdata_name)
            prettyprint(refdata.refdata_info())
    elif cmd == 'create':
        for refdata_name in refdata_names:
            refdata = RefData(refdata_name)
            refdata.create(dryrun)
    elif cmd == 'fetch':
        for refdata_name in refdata_names:
            refdata = RefData(refdata_name)
            refdata.fetch_categories(cat, key, dryrun, force)

if __name__ == '__main__':
    main()

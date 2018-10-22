#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Reference data (refdata) module

Note that the term "refdata" is used to mean both individual data sources, as well as the
overall topic of reference (i.e. external) data for CMIR (we could use "refdatasrc" for
the former, but it's kind of cumbersome, so we'll deal with the ambiguity, and resolve
in context/situ)
"""

import os.path
import regex as re
import json
import glob
import datetime as dt
from time import sleep
import logging

from bs4 import BeautifulSoup
import requests

from core import BASE_DIR, cfg, env, log, dbg_hand, DFLT_FETCH_INT, DFLT_HTML_PARSER
from utils import LOV, prettyprint, strtype, collecttype
from musiclib import MusicLib, normalize_name, NormFlag, NAME_RE, ROLE_RE, ROLE_RE2

#####################
# core/config stuff #
#####################

REFDATA_BASE = cfg.config('refdata_base')
REFDATA      = cfg.config('refdata')

##############################
# common constants/functions #
##############################

ConfigKey      = LOV(['URL_FMT',
                      'CHARSET',
                      'CATEGORIES',
                      'HTTP_HEADERS',
                      'FETCH_INTERVAL'], 'lower')
REQUIRED_ATTRS = {ConfigKey.URL_FMT,
                  ConfigKey.CATEGORIES}

# the following correspond to hardwired RefData member variables
INFO_KEYS      = {'name',
                  'status',
                  'config',
                  'state',
                  'categories'}
# if any of the info keys should not be dumped to log file
NOPRINT_KEYS   = {'categories'}

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
        self.html_parser = env.get('html_parser') or DFLT_HTML_PARSER
        self.charset = self.config.get(ConfigKey.CHARSET)
        log.debug("HTML parser: %s, charset: %s" % (self.html_parser, self.charset))

        # these are the directories/files for the current refdata source
        self.refdata_dir       = os.path.join(BASE_DIR, 'refdata', self.name)
        self.refdata_info_file = os.path.join(self.refdata_dir, 'refdata_info.json')
        self.category_dirs     = {cat: os.path.join(self.refdata_dir, cat)
                                  for cat in self.config[ConfigKey.CATEGORIES].keys()}
        self.http_headers      = self.config.get(ConfigKey.HTTP_HEADERS)
        fetch_interval         = self.config.get(ConfigKey.FETCH_INTERVAL, DFLT_FETCH_INT)
        self.fetch_delta       = dt.timedelta(0, fetch_interval)
        self.sess              = requests.Session()

        self.state = None
        self.categories = None
        self.load_state()
        if self.status == Status.NEW:
            self.status = Status.ACTIVE if self.valid() else Status.INVALID

        self.last_fetch = dt.datetime.utcnow() - self.fetch_delta

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
        log.debug("Storing state for %s\n%s" % (self.name, prettyprint(self.refdata_info(exclude=NOPRINT_KEYS), noprint=True)))

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
        log.debug("Loading state for %s\n%s" % (self.name, prettyprint(self.refdata_info(exclude=NOPRINT_KEYS), noprint=True)))

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
            m = re.fullmatch(r'([a-z])-([a-z])', key.lower())
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
        if elapsed < self.fetch_delta:
            sleep_delta = self.fetch_delta - elapsed
            sleep((sleep_delta).seconds + (sleep_delta).microseconds / 1000000.0)

        catdata_url = self.build_url(cat, key)
        log.debug("Fetching from %s (headers: %s)" % (catdata_url, self.http_headers))
        r = self.sess.get(catdata_url, headers=self.http_headers)
        catdata_content = r.content

        self.last_fetch = dt.datetime.utcnow()
        # TODO: this really needs to return metadata around the catdata, not
        # just the contents!!!
        return catdata_content

    def parse(self, cat, key, dryrun, force):
        """
        """
        cats = [cat] if not collecttype(cat) else cat
        if strtype(key):
            m = re.fullmatch(r'([a-z])-([a-z])', key.lower())
            if m and ord(m.group(1)) <= ord(m.group(2)):
                keys = [chr(charcode) for charcode in range(ord(m.group(1)), ord(m.group(2)) + 1)]
            else:
                keys = [key]
        else:
            keys = [key] if not collecttype(key) else key

        log.debug("Parsing refdata for categories \"%s\" and keys \"%s\"" % (cats, keys))
        for cat in cats:
            for key in keys:
                catdata_name = self.catdata_name(cat, key)
                catdata_file = self.catdata_file(cat, key)
                with open(catdata_file, encoding=self.charset) as f:
                    soup = BeautifulSoup(f, self.html_parser)

                holder = soup.find('div', id="namelist_holder")
                # note that "most-popular" may contain items not in the alphabetized letterchuck
                # sections (e.g. due to leading non-alpha characters)--would be nice to discover
                # a more complete index of some kind!
                chunk = holder.find('div', id="most-popular")
                if chunk:
                    self.parse_chunk(cat, chunk)
                # this is probably the next div in most (if not all) cases, but let's just
                # locate separately to be pedantic
                chunk = holder.find('div', id="letterchunk-1")
                if chunk:
                    while chunk:
                        self.parse_chunk(cat, chunk)
                        chunk = chunk.find_next_sibling('div')
                else:
                    # items are contained directly in holder for sparse listings
                    self.parse_chunk(cat, holder)

    def parse_chunk(self, cat, chunk):
        """
        """
        PERSON_CAT = {'composers', 'conductors', 'performers'}
        ml = MusicLib()

        for item in chunk.ul('li', recursive=False):
            name      = item.a.string.strip()
            href      = item.a['href']
            ent_name  = None
            ent_type  = cat.rstrip('s')
            addl_ref  = None
            alt_names = set()
            raw_name  = None

            # REVISIT: the original "raw" string is lost for these special cases--we should
            # consider whether these are worth preserving or not (since they are presumably
            # aberrations to begin with)!!!
            if '[' in name:
                # can be liberal in parsing here (compared to special case below)
                m = re.fullmatch(r'(.+) \[(.*)\]', name)
                if m:
                    name = m.group(1)
                    addl_ref = m.group(2)
                if not m:
                    # special case (for bad formatting somewhere upstream):
                    #   "Keckler, Vocals] Joseph [Piano" -> "Keckler, Joseph [Piano/Vocals]"
                    pattern = r'(%s), (%s)\] (%s) \[(%s)' % (NAME_RE, ROLE_RE, NAME_RE, ROLE_RE)
                    m = re.fullmatch(pattern, name)
                    if m:
                        name = "%s, %s" % (m.group(1), m.group(3))
                        addl_ref = "%s/%s" % (m.group(4), m.group(2))
                if not m:
                    # special case (for bad formatting somewhere upstream):
                    #   "Bilan, Jr. [Xylophone] Ladislav" -> "Ladislav Bilan, Jr. [Xylophone]"
                    pattern = r'(%s) \[(%s)\],? (%s)' % (NAME_RE, ROLE_RE2, NAME_RE)
                    m = re.fullmatch(pattern, name)
                    if m:
                        name = "%s, %s" % (m.group(1), m.group(3))
                        addl_ref = m.group(2)

            if '&' in name:
                # special case: "Jenkins, Gordon & His Orchestra" (just do a rough parse)
                pattern = r'(%s), (%s) (& .+)' % (NAME_RE, NAME_RE)
                m = re.fullmatch(pattern, name)
                if m:
                    name = "%s %s %s" % (m.group(2), m.group(1), m.group(3))

            if cat in PERSON_CAT:
                ent_name, alt_names, raw_name = normalize_name(name, NormFlag.INCL_SELF)
            else:
                ent_name = name
                raw_name = name
            href = re.sub(r';jsessionid=\w+', '', href)
            m = re.search(r'\((\d+)\)', item.contents[1])
            recs = int(m.group(1)) if m else 0

            log.debug("REFLIB: %s \"%s\" [%s]" % (cat, name, href))
            if ent_name:
                log.debug("        entity name: %s" % (ent_name))
            if alt_names:
                log.debug("        alt names: %s" % (alt_names))
            if addl_ref:
                log.debug("        addl ref: %s" % (addl_ref))
            if recs:
                log.debug("        recordings: %d" % (recs))

            ent_data = {'entity_ref'      : ent_name,
                        'entity_type'     : ent_type,
                        'ref_source'      : self.name,
                        'addl_ref'        : addl_ref,
                        'source_data'     : [str(c) for c in item.children],
                        'is_entity'       : True,
                        'mstr_entity_name': ent_name,
                        'entity_strength' : recs,
                        'ref_strength'    : 100}
            # FIX: for now we're only using true and null as values for booleans, this is
            # kind of stupid, but we need to be consistent across the entire database!!!
            if raw_name and ent_name == raw_name:
                ent_data['is_raw'] = True

            er_recs = ml.insert_entity_ref(self, ent_data, alt_names, raw_name)

#####################
# command line tool #
#####################

import click

@click.command()
@click.option('--list',      'cmd', flag_value='list', default=True, help="List all (or specified) refdata sources, and their categories")
@click.option('--create',    'cmd', flag_value='create', help="Create new refdata source (skip if refdata source exists)")
@click.option('--fetch',     'cmd', flag_value='fetch', help="Fetch data by category for refdata source")
@click.option('--parse',     'cmd', flag_value='parse', help="Parse refdata and write to entity_ref")
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
    elif cmd == 'parse':
        for refdata_name in refdata_names:
            refdata = RefData(refdata_name)
            refdata.parse(cat, key, dryrun, force)

if __name__ == '__main__':
    main()

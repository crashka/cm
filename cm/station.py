#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Station class/functions
"""

import os
import re
import json
import glob
import datetime as dt
from time import sleep
import logging

import yaml
import click
import requests

from utils import prettyprint

################
# config stuff #
################

FILE_DIR     = os.path.dirname(os.path.realpath(__file__))
BASE_DIR     = os.path.realpath(os.path.join(FILE_DIR, os.pardir))
CONFIG_DIR   = 'config'
CONFIG_FILE  = 'config.yml'
CONFIG_PATH  = os.path.join(BASE_DIR, CONFIG_DIR, CONFIG_FILE)
cfg_profiles = dict()  # {profile_name: {section_name: ...}}

def config(section, profile = None):
    """Get config section for specified profile

    :param section: section within profile (or 'default')
    :param profile: [optional] if specified, overlay entries on top of 'default' profile
    :return: dict indexed by key
    """
    global cfg_profiles
    if profile in cfg_profiles:
        return cfg_profiles[profile].get(section, {})

    with open(CONFIG_PATH, 'r') as f:
        cfg = yaml.safe_load(f)
    if cfg:
        prof_data = cfg.get('default', {})
        if profile:
            prof_data.update(cfg.get(profile, {}))
        cfg_profiles[profile] = prof_data
    else:
        cfg_profiles[profile] = {}

    return cfg_profiles[profile].get(section, {})

##############################
# common constants/functions #
##############################

STATIONS       = config('stations')
REQUIRED_ATTRS = set(['url_fmt',
                      'date_fmt',
                      'playlist_ext'])
STD_DATE_FMT   = '%Y-%m-%d'  # same as ISO 8601
INFO_KEYS      = set(['name',
                      'info',
                      'state',
                      'playlists',
                      'shows'])

# kindly internet fetch interval
FETCH_INT      = 2.0
FETCH_DELTA    = dt.timedelta(0, FETCH_INT)

# create logger
LOGGER_NAME = 'cm'
LOG_DIR     = 'log'
LOG_FILE    = LOGGER_NAME + '.log'
LOG_PATH    = os.path.join(BASE_DIR, LOG_DIR, LOG_FILE)
LOG_FMTR    = logging.Formatter('%(asctime)s %(levelname)s [%(filename)s:%(lineno)s]: %(message)s')

dlft_hand = logging.FileHandler(LOG_PATH)
dlft_hand.setLevel(logging.DEBUG)
dlft_hand.setFormatter(LOG_FMTR)

dbg_hand = logging.StreamHandler()
dbg_hand.setLevel(logging.DEBUG)
dbg_hand.setFormatter(LOG_FMTR)

log = logging.getLogger(LOGGER_NAME)
log.setLevel(logging.INFO)
log.addHandler(dlft_hand)

# requests session
sess = requests.Session()

#################
# Station class #
#################

class Station(object):
    """
    """
    @staticmethod
    def list():
        """
        """
        dirs = glob.glob(os.path.join(BASE_DIR, 'stations', '*'))
        return sorted([os.path.basename(dir) for dir in dirs])

    def __init__(self, name):
        """
        """
        log.info("instantiating Station(%s)" % (name))
        self.name = name
        self.info = STATIONS.get(name)
        if not self.info:
            raise RuntimeError("Station \"%s\" not known" % (name))
        for attr in REQUIRED_ATTRS:
            if attr not in self.info:
                raise RuntimeError("Required config attribute \"%s\" missing for \"%s\"" % (attr, name))

        # extract tokens in url_fmt upfront
        self.tokens = re.findall(r'(\<[A-Z_]+\>)', self.url_fmt)
        if not self.tokens:
            raise RuntimeError("No tokens in URL format string for \"%s\"" % (name))

        self.station_dir       = os.path.join(BASE_DIR, 'stations', self.name)
        self.station_info_file = os.path.join(self.station_dir, 'station_info.json')
        self.playlists_file    = os.path.join(self.station_dir, 'playlists.json')
        self.playlist_dir      = os.path.join(self.station_dir, 'playlists')
        self.playlist_min      = self.info.get('playlist_min')
        self.http_headers      = self.info.get('http_headers', {})

        self.state = None
        self.playlists = None
        self.load_state()

        self.last_fetch = dt.datetime.utcnow() - FETCH_DELTA
        
        # TEMP: just for initial dev/testing!!!
        self.todays_date = self.build_date(dt.date.today())
        self.todays_url = self.build_url(dt.date.today())

    def __getattr__(self, key):
        try:
            return self.info[key]
        except KeyError:
            raise AttributeError

    def station_info(self):
        return {k: v for k, v in self.__dict__.items() if k in INFO_KEYS}

    def store_state(self):
        """
        """
        with open(self.station_info_file, 'w') as f:
            json.dump(self.station_info(), f, indent=2)
        with open(self.playlists_file, 'w') as f:
            json.dump(self.playlists, f, indent=2)

    def load_state(self, force=False):
        """
        """
        if self.state is None or force:
            self.state = {}
            if os.path.isfile(self.station_info_file) and os.path.getsize(self.station_info_file) > 0:
                with open(self.station_info_file) as f:
                    station_info = json.load(f)
                self.state.update(station_info.get('state', {}))

        if self.playlists is None or force:
            self.playlists = {}
            if os.path.isfile(self.playlists_file) and os.path.getsize(self.playlists_file) > 0:
                with open(self.playlists_file) as f:
                    playlists = json.load(f)
                self.playlists.update(playlists)

    def build_date(self, date):
        """Builds date string based on date_fmt, which is a required attribute in the station info
        """
        return date.strftime(self.date_fmt).lower()

    def build_url(self, date):
        """Builds playlist URL based on url_fmt, which is a required attribute in the station info
        """
        # this is a magic variable name that matches a URL format token
        date_str = self.build_date(date)
        url = self.url_fmt
        for token in self.tokens:
            token_var = token[1:-1].lower()
            value = vars().get(token_var) or getattr(self, token_var, None)
            if not value:
                raise RuntimeError("Token attribute \"%s\" not found for \"%s\"" % (token_var, self.name))
            url = url.replace(token, value)

        return url

    def check(self, validate=False):
        """Return True if station exists (and passes validation test, if requested)
        """
        created = os.path.exists(self.station_dir) and os.path.isdir(self.station_dir)
        if not validate:
            return created
        else:
            return created and self.valid()

    def valid(self):
        """Return True if station is validated
        """
        # TEMP: for now, just check integrity of directory structure!!!
        valid = os.path.exists(self.playlist_dir) and os.path.isdir(self.playlist_dir)
        return valid

    def create(self, dryrun=False):
        """Create station (raise exception if it already exists)
        """
        if self.check():
            raise RuntimeError("Station \"%s\" already exists" % (self.name))
        self.state = {}
        self.playlists = {}
        if not dryrun:
            os.mkdir(self.station_dir)
            os.mkdir(self.playlist_dir)
            self.store_state()
        else:
            print(self.__dict__)

    def playlist_name(self, date):
        """
        """
        return date.strftime(STD_DATE_FMT)

    def playlist_file(self, date):
        """
        """
        filename = self.playlist_name(date) + '.' + self.playlist_ext
        return os.path.join(self.playlist_dir, filename)

    def get_playlists(self, start_date=None, num=1):
        """Return names of playlists (from playlists directory)
        """
        files = glob.glob(os.path.join(self.playlist_dir, '*'))
        return {os.path.splitext(os.path.basename(file))[0] for file in files}

    def validate_playlists(self):
        """
        """
        fs_playlists = self.get_playlists()
        diff = fs_playlists.symmetric_difference(self.playlists.keys())
        if len(diff) > 0:
            raise RuntimeError("Inconsistent playlist file/metadata for: " + str(sorted(diff)))

        if len(fs_playlists) == 0:
            return

        min_date = dt.datetime.strptime(min(fs_playlists), STD_DATE_FMT).date()
        max_date = dt.datetime.strptime(max(fs_playlists), STD_DATE_FMT).date()
        ord_list = range(min_date.toordinal(), max_date.toordinal())
        all_dates = {dt.date.fromordinal(ord).strftime(STD_DATE_FMT) for ord in ord_list}
        missing = all_dates.difference(fs_playlists)
        if len(missing) > 0:
            raise RuntimeError("Missing playlists for: " + str(sorted(missing)))
    
    def fetch_playlists(self, start_date, num=1, dryrun=False):
        """Write specified playlists to filesystem
        """
        start_ord = start_date.toordinal()
        end_ord   = start_ord + num
        ord_step  = (1, -1)[num < 0]
        for ord in range(start_ord, end_ord, ord_step):
            date = dt.date.fromordinal(ord)
            playlist_name = self.playlist_name(date)
            playlist_file = self.playlist_file(date)
            if os.path.exists(playlist_file) and os.path.getsize(playlist_file) > 0:
                log.info("Skipping fetch for \"%s\", file exists" % (playlist_name))
                continue
            playlist_text = self.fetch_playlist(date)
            if dryrun:
                prettyprint(playlist_text)
            else:
                with open(playlist_file, 'w') as f:
                    f.write(playlist_text)
                status = 'ok'
                if self.playlist_min and len(playlist_text) < self.playlist_min:
                    log.warning("Playlist \"%s\" content length %d below min" % (playlist_name, len(playlist_content)))
                    status = 'notok'
                self.playlists[playlist_name] = {
                    'file': playlist_file,
                    'size': len(playlist_text),
                    'status': status
                }
                log.debug("Content for playlist \"%s\": %s..." % (playlist_name, playlist_text[:250]))

        if not dryrun:
            self.store_state()

    def fetch_playlist(self, date, dummy=False):
        """Fetch playlist information from internet
        """
        elapsed = dt.datetime.utcnow() - self.last_fetch
        if elapsed < FETCH_DELTA:
            sleep_delta = FETCH_DELTA - elapsed
            sleep((sleep_delta).seconds + (sleep_delta).microseconds / 1000000.0)

        if not dummy:
            r = sess.get(self.build_url(date), headers=self.http_headers)
            playlist_content = r.content
        else:
            doc = {
                'name': self.playlist_name(date),
                'file': self.playlist_file(date),
                'url':  self.build_url(date)
            }
            playlist_content = json.dumps(doc)

        self.last_fetch = dt.datetime.utcnow()
        # TODO: this really needs to return metadata around the playlist, not
        # just the contents!!!
        return playlist_content

#####################
# command line tool #
#####################

@click.command()
@click.option('--list',      'cmd', flag_value='list', default=True, help="List all (or specified) stations")
@click.option('--create',    'cmd', flag_value='create', help="Create new station (skip if station exists)")
@click.option('--playlists', 'cmd', flag_value='playlists', help="List playlists for station (fail if station does not exist)")
@click.option('--fetch',     'cmd', flag_value='fetch', help="Fetch playlists for station (fail if station does not exist)")
@click.option('--validate',  'cmd', flag_value='validate', help="Validate playlist metadata for station (fail if station does not exist)")
@click.option('--date',      help="Start date to list, fetch, validate (format: Y-m-d)")
@click.option('--num',       default=1, help="Number of dates to list, fetch, or validate (positive indicates forward in time from start date, negative indicates backward in time)")
@click.option('--skip',      is_flag=True, help="Skip (rather than fail) if station does not exist")
@click.option('--force',     is_flag=True, help="Overwrite existing playlists (otherwise skip over), applies only to --fetch")
@click.option('--dryrun',    is_flag=True, help="Do not execute write, log to INFO level instead")
@click.option('--debug',     default=0, help="Debug level")
@click.argument('name',      default='all', required=True)
def main(cmd, date, num, skip, force, dryrun, debug, name):
    """Manage station information for comma-separated list of station names (or 'all')
    """
    if debug > 0:
        log.setLevel(logging.DEBUG)
        log.addHandler(dbg_hand)

    if name == 'all':
        station_names = STATIONS.keys()
    else:
        station_names = name.upper().split(',')

    if cmd == 'list':
        station_list = Station.list()
        for station_name in station_list:
            if station_name in station_names:
                station = Station(station_name)
                prettyprint(station.station_info())
    if cmd == 'create':
        for station_name in station_names:
            station = Station(station_name)
            station.create(dryrun)
    elif cmd == 'playlists':
        for station_name in station_names:
            station = Station(station_name)
            playlists = station.get_playlists()
            prettyprint(sorted(playlists))
    elif cmd == 'fetch':
        try:
            start_date = dt.datetime.strptime(date, STD_DATE_FMT).date()
        except ValueError:
            raise RuntimeError("Invalid date to fetch")

        for station_name in station_names:
            station = Station(station_name)
            station.fetch_playlists(start_date, num, dryrun)
    elif cmd == 'validate':
        for station_name in station_names:
            station = Station(station_name)
            station.validate_playlists()

if __name__ == '__main__':
    main()

#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Station module
"""

import os
import re
import json
import glob
import datetime as dt
import pytz
from time import sleep
from enum import Enum
import logging
import logging.handlers

import click
import requests

from utils import Config, unixtime, prettyprint

################
# config stuff #
################

FILE_DIR     = os.path.dirname(os.path.realpath(__file__))
BASE_DIR     = os.path.realpath(os.path.join(FILE_DIR, os.pardir))
CONFIG_DIR   = 'config'
CONFIG_FILE  = 'config.yml'
CONFIG_PATH  = os.path.join(BASE_DIR, CONFIG_DIR, CONFIG_FILE)
cfg          = Config(CONFIG_PATH)

##############################
# common constants/functions #
##############################

STATIONS       = cfg.config('stations')
REQUIRED_ATTRS = set(['url_fmt',
                      'date_fmt',
                      'playlist_ext'])
STD_DATE_FMT   = '%Y-%m-%d'  # same as ISO 8601
INFO_KEYS      = set(['name',
                      'status',
                      'config',
                      'state',
                      'playlists',
                      'shows'])

# TODO: remove playlists from station_info!!!
NOPRINT_KEYS   = set(['playlists'])

# REVISIT: with python3, may need to ditch native enums (since value is not implicit
# in member reference)!!!
class Status(Enum):
    """Station status values
    """
    UNKNOWN  = 'unknown'
    CREATED  = 'created'
    ACTIVE   = 'active'
    INVALID  = 'invalid'
    DISABLED = 'disabled'

class Fetch(Enum):
    """Fetch target special values
    """
    CATCHUP  = 'catchup'
    MISSING  = 'missing'
    INVALID  = 'invalid'

class State(Enum):
    """State structure fields
    """
    TOTAL    = 'total'
    EARLIEST = 'earliest'
    LATEST   = 'latest'
    VALID    = 'valid'
    MISSING  = 'missing'
    INVALID  = 'invalid'

class PlaylistAttr(Enum):
    """Attributes for playlists info section
    """
    FILE     = 'file'
    SIZE     = 'size'
    STATUS   = 'status'

class PlaylistStatus(Enum):
    """Playlist status values
    """
    OK       = 'ok'
    NOTOK    = 'notok'

# kindly internet fetch interval (TODO: move to config file!!!)
FETCH_INT    = 2.0
FETCH_DELTA  = dt.timedelta(0, FETCH_INT)

# create logger (TODO: logging parameters belong in config file as well!!!)
LOGGER_NAME  = 'cm'
LOG_DIR      = 'log'
LOG_FILE     = LOGGER_NAME + '.log'
LOG_PATH     = os.path.join(BASE_DIR, LOG_DIR, LOG_FILE)
LOG_FMTR     = logging.Formatter('%(asctime)s %(levelname)s [%(filename)s:%(lineno)s]: %(message)s')
LOG_FILE_MAX = 50000000
LOG_FILE_NUM = 99

dlft_hand = logging.handlers.RotatingFileHandler(LOG_PATH, 'a', LOG_FILE_MAX, LOG_FILE_NUM)
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

def str2date(datestr, fmt = STD_DATE_FMT):
    """Returns dt.date object
    """
    return dt.datetime.strptime(datestr, fmt).date()

#################
# Station class #
#################

class Station(object):
    """Represents a station defined in config.yml
    """
    @staticmethod
    def list():
        """List stations that have been created

        :return: sorted list of station names (same as directory names)
        """
        dirs = glob.glob(os.path.join(BASE_DIR, 'stations', '*'))
        return sorted([os.path.basename(dir) for dir in dirs])

    def __init__(self, name):
        """Sets status field locally (but not written back to info file)
        """
        log.debug("Instantiating Station(%s)" % (name))
        self.name = name
        self.status = Status.CREATED
        self.config = STATIONS.get(name)
        if not self.config:
            raise RuntimeError("Station \"%s\" not known" % (name))
        for attr in REQUIRED_ATTRS:
            if attr not in self.config:
                raise RuntimeError("Required config attribute \"%s\" missing for \"%s\"" % (attr, name))

        # extract tokens in url_fmt upfront
        self.tokens = re.findall(r'(\<[A-Z_]+\>)', self.url_fmt)
        if not self.tokens:
            raise RuntimeError("No tokens in URL format string for \"%s\"" % (name))

        self.station_dir       = os.path.join(BASE_DIR, 'stations', self.name)
        self.station_info_file = os.path.join(self.station_dir, 'station_info.json')
        self.playlists_file    = os.path.join(self.station_dir, 'playlists.json')
        self.playlist_dir      = os.path.join(self.station_dir, 'playlists')
        self.playlist_min      = self.config.get('playlist_min')
        self.http_headers      = self.config.get('http_headers', {})

        self.state = None
        self.playlists = None
        self.load_state()
        if self.status == Status.CREATED:
            self.status = Status.ACTIVE if self.valid() else Status.INVALID

        self.last_fetch = dt.datetime.utcnow() - FETCH_DELTA
        
        # TEMP: just for initial dev/testing!!!
        self.todays_date = self.build_date(dt.date.today())
        self.todays_url = self.build_url(dt.date.today())

    def __getattr__(self, key):
        try:
            return self.config[key]
        except KeyError:
            raise AttributeError()

    def station_info(self, keys = INFO_KEYS, exclude = None):
        """Return station info (canonical fields) as a dict comprehension
        """
        if type(keys) not in (set, list, tuple):
            keys = [keys]
        elif type(exclude) == set and type(keys) == set:
            keys = keys - exclude
        return {k: v for k, v in self.__dict__.items() if k in keys}

    def store_state(self):
        """Writes station info (canonical fields) to station_info.json file
        """
        with open(self.station_info_file, 'w') as f:
            json.dump(self.station_info(), f, indent=2)
        with open(self.playlists_file, 'w') as f:
            json.dump(self.playlists, f, indent=2)
        log.debug("Storing state for %s\n%s" % (self.name, prettyprint(self.station_info(exclude=NOPRINT_KEYS), noprint=True)))

    def load_state(self, force = False):
        """Loads station info (canonical fields) from station_info.json file
        """
        if self.state is None or force:
            self.state = {}
            if os.path.isfile(self.station_info_file) and os.path.getsize(self.station_info_file) > 0:
                with open(self.station_info_file) as f:
                    station_info = json.load(f)
                self.state.update(station_info.get('state', {}))
        log.debug("Loading state for %s\n%s" % (self.name, prettyprint(self.station_info(exclude=NOPRINT_KEYS), noprint=True)))

        if self.playlists is None or force:
            self.playlists = {}
            if os.path.isfile(self.playlists_file) and os.path.getsize(self.playlists_file) > 0:
                with open(self.playlists_file) as f:
                    playlists = json.load(f)
                self.playlists.update(playlists)
        log.debug("Loading playlists for %s" % (self.name))


    def build_date(self, date):
        """Builds date string based on date_fmt, which is a required attribute in the station info

        :param date: datetime.date
        :return: string
        """
        return date.strftime(self.date_fmt).lower()

    def build_url(self, date):
        """Builds playlist URL based on url_fmt, which is a required attribute in the station info

        :param date: datetime.date
        :return: string
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

    def check(self, validate = False):
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

    def create(self, dryrun = False):
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
        """Playlist name is just the standard date string
        """
        return date.strftime(STD_DATE_FMT)

    def playlist_file(self, date):
        """Playlist file is playlist name plus data representation extension
        """
        filename = self.playlist_name(date) + '.' + self.playlist_ext
        return os.path.join(self.playlist_dir, filename)

    def get_playlists(self, start_date = None, num = 1):
        """Return names of playlists (from playlists directory)

        :return: set
        """
        files = glob.glob(os.path.join(self.playlist_dir, '*'))
        return {os.path.splitext(os.path.basename(file))[0] for file in files}

    def validate_playlists(self, dryrun = False):
        """Cross-checks playlist files from file system with playlists structure in station info,
        looks for missing playlists, and writes state structure back to station_info.json file
        """
        fs_playlists = self.get_playlists()
        diff = fs_playlists.symmetric_difference(self.playlists.keys())
        if len(diff) > 0:
            raise RuntimeError("Inconsistent playlist file/metadata for: " + str(sorted(diff)))

        if len(fs_playlists) == 0:
            return

        min_date = str2date(min(fs_playlists))
        max_date = str2date(max(fs_playlists))
        ord_list = range(min_date.toordinal(), max_date.toordinal())
        all_dates = {dt.date.fromordinal(ord).strftime(STD_DATE_FMT) for ord in ord_list}
        missing = all_dates.difference(fs_playlists)
        if len(missing) > 0:
            raise RuntimeError("Missing playlists for: " + str(sorted(missing)))

        # REVISIT: do we really want to just overwrite???
        self.state = {
            State.TOTAL    : len(self.playlists),
            State.EARLIEST : min(fs_playlists),
            State.LATEST   : max(fs_playlists),
            # TEMP: None means we don't know!
            State.VALID    : None,
            State.MISSING  : None,
            State.INVALID  : None
        }
        log.debug("Validating playlist info for %s" % (self.name))

        if not dryrun:
            self.store_state()
    
    def fetch_playlists(self, start_date, num = 1, dryrun = False):
        """Write specified playlists to filesystem
        """
        if type(start_date) in (unicode, str):
            try:
                if start_date == Fetch.CATCHUP:
                    self.validate_playlists(dryrun=True)  # no need to store state yet
                    start_date = str2date(self.state.get(State.LATEST)) + dt.timedelta(1)
                    tz = pytz.timezone(self.time_zone)
                    today = dt.datetime.now(tz).date()
                    num = (today - start_date).days
                    if num < 0:
                        raise RuntimeError("Latest playlist newer than yesterday")
                elif start_date == Fetch.MISSING:
                    raise ValueError("Not yet implemented")
                elif start_date == Fetch.INVALID:
                    raise ValueError("Not yet implemented")
                else:
                    start_date = str2date(start_date)
            except ValueError:
                raise RuntimeError("Invalid date to fetch")

        if num == 0:
            log.debug("No playlists to fetch for %s", (self.name))
            return  # nothing to do

        # do this using ordinals instead of timedelta, since the math syntax is more native
        # (and hence logic more dependable)
        start_ord = start_date.toordinal()
        end_ord   = start_ord + num
        ord_step  = (1, -1)[num < 0]
        log.debug("Fetching %d playlist(s) starting with %s" % (num, start_date.strftime(STD_DATE_FMT)))
        for ord in range(start_ord, end_ord, ord_step):
            date = dt.date.fromordinal(ord)
            playlist_name = self.playlist_name(date)
            playlist_file = self.playlist_file(date)
            # TODO: allow forcing an overwrite!!!
            if os.path.exists(playlist_file) and os.path.getsize(playlist_file) > 0:
                log.info("Skipping fetch for \"%s\", file exists" % (playlist_name))
                continue
            playlist_text = self.fetch_playlist(date)
            log.debug("Content for playlist \"%s\": %s..." % (playlist_name, playlist_text[:250]))
            if not dryrun:
                with open(playlist_file, 'w') as f:
                    f.write(playlist_text)
                status = PlaylistStatus.OK
                if self.playlist_min and len(playlist_text) < self.playlist_min:
                    log.warning("Playlist \"%s\" content length %d below min" % (playlist_name, len(playlist_content)))
                    status = PlaylistStatus.NOTOK
                self.playlists[playlist_name] = {
                    PlaylistAttr.FILE   : os.path.relpath(playlist_file, self.station_dir),
                    PlaylistAttr.SIZE   : len(playlist_text),
                    PlaylistAttr.STATUS : status
                }
            self.validate_playlists(dryrun)  # implicitly stores state, if not dryrun

    def fetch_playlist(self, date, dummy = False):
        """Fetch single playlist information (e.g. from internet)
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
                'type': 'dummy',
                'name': self.playlist_name(date),
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
        for station_name in station_names:
            station = Station(station_name)
            prettyprint(station.station_info(exclude=NOPRINT_KEYS))
    if cmd == 'create':
        for station_name in station_names:
            station = Station(station_name)
            station.create(dryrun)
    elif cmd == 'playlists':
        for station_name in station_names:
            station = Station(station_name)
            prettyprint(sorted(station.get_playlists()))
    elif cmd == 'fetch':
        for station_name in station_names:
            station = Station(station_name)
            station.fetch_playlists(date, num, dryrun)
    elif cmd == 'validate':
        for station_name in station_names:
            station = Station(station_name)
            station.validate_playlists(dryrun)

if __name__ == '__main__':
    main()

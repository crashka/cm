#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Station module
"""

import os.path
import regex as re
import json
import glob
import datetime as dt
from zoneinfo import ZoneInfo
from time import sleep
import base64
import logging

import requests

from .utils import LOV, prettyprint, str2date, date2str
from .core import BASE_DIR, cfg, log, dbg_hand, DFLT_FETCH_INT, ObjCollect
from .playlist import Playlist
from .parser import Parser

################
# config stuff #
################

STATION_BASE = cfg.config('station_base')
STATIONS     = cfg.config('stations')

##############################
# common constants/functions #
##############################

ConfigKey      = LOV(['URLS',
                      'COND',
                      'URL_FMT',
                      'DATE_FMT',
                      'DATE_FMT2',
                      'DATE_FUNC',
                      'DATE_METH',
                      'TIMEZONE',
                      'EPOCH',
                      'PLAYLIST_EXT',
                      'PLAYLIST_MIN',
                      'HTTP_HEADERS',
                      'FETCH_INTERVAL',
                      'PARSER_PKG',
                      'PARSER_CLS',
                      'SYND_LEVEL'], 'lower')
REQD_CFG_ATTRS = {ConfigKey.TIMEZONE,
                  ConfigKey.PLAYLIST_EXT,
                  ConfigKey.PARSER_PKG,  # could make this optional, but probably not worth it
                  ConfigKey.PARSER_CLS}
URL_ATTRS      = {ConfigKey.COND,
                  ConfigKey.URL_FMT,
                  ConfigKey.DATE_FMT,
                  ConfigKey.DATE_FMT2,
                  ConfigKey.DATE_FUNC,
                  ConfigKey.DATE_METH}
REQD_URL_ATTRS = {ConfigKey.COND,
                  ConfigKey.URL_FMT,
                  ConfigKey.DATE_FMT}

DEFAULT_COND   = 'default'

# the following correspond to hardwired Station member variables
INFO_KEYS      = ('name',
                  'status',
                  'config',
                  'state')
# if any of the info keys should not be dumped to log file
NOPRINT_KEYS   = set()

# local module LOVs
Status         = LOV(['NEW',
                      'ACTIVE',
                      'INVALID',
                      'DISABLED'], 'lower')
FetchTarg      = LOV(['CATCHUP',
                      'MISSING',
                      'INVALID',
                      'OLDER'], 'lower')
StateAttr      = LOV(['TOTAL',
                      'EARLIEST',
                      'LATEST',
                      'EPOCH',
                      'VALID',
                      'MISSING',
                      'INVALID'], 'lower')
PlaylistAttr   = LOV(['FILE',
                      'SIZE',
                      'STATUS'], 'lower')
PlaylistStatus = LOV(['OK',
                      'NOTOK'], 'lower')

#####################
# Special functions #
#####################

def base64encode(datestr):
    """Wrapper for b64encode that works directly on strings

    :param datestr: string
    "return" string
    """
    return base64.b64encode(datestr.encode()).decode()

def wcpe_special(datestr):
    """Transform date string for WCPE

    Need to roll back the date portion to the prior Monday (i.e. their start of the week),
    and downcase the day of week string (e.g. '2018-09-17/Tuesday' -> 2018-09-16/tuesday')

    :param datestr: format '%Y-%m-%d/%A'
    :return: string
    """
    (ymd, dow) = datestr.split('/')  # can assume this always succeeds, since we built it
    date = str2date(ymd)
    days_since_mon = int(date.strftime('%u')) - 1  # %u: 1..7 = Mon..Sun
    mon = date - dt.timedelta(days_since_mon)
    return date2str(mon) + '/' + dow.lower()

#################
# Station class #
#################

class Station:
    """Represents a station defined in config.yml
    """
    @staticmethod
    def list() -> list[str]:
        """List stations that have been created

        :return: sorted list of station names (same as directory names)
        """
        dirs = glob.glob(os.path.join(BASE_DIR, 'stations', '*'))
        return sorted([os.path.basename(dir_) for dir_ in dirs])

    def __init__(self, name: str):
        """Sets status field locally (but not written back to info file)
        """
        if name not in STATIONS:
            raise RuntimeError("Station \"%s\" not known" % name)
        log.debug("Instantiating Station(%s)" % name)
        self.name = name
        self.status = Status.NEW
        self.config = STATION_BASE.copy()
        self.config.update(STATIONS[name])
        missing = REQD_CFG_ATTRS - self.config.keys()
        if missing:
            raise RuntimeError("Required config attribute(s) %s missing for \"%s\"" %
                               (missing, name))
        if not self.config.get(ConfigKey.URLS):
            url_info = {k: self.config[k] for k in URL_ATTRS & self.config.keys()}
            url_info[ConfigKey.COND] = DEFAULT_COND
            self.urls = [url_info]
        # IMPORTANT: from here on, we refer only to self.urls (not self.config[ConfigKey.URLS]),
        # since it covers both multiple- and single- (i.e. default) URL configurations!!!
        for url_info in self.urls:
            url_missing = REQD_URL_ATTRS - url_info.keys()
            if url_missing:
                raise RuntimeError("Required URL attribute(s) %s missing for \"%s\"" %
                                   (url_missing, name))

        self.station_dir       = os.path.join(BASE_DIR, 'stations', self.name)
        self.station_info_file = os.path.join(self.station_dir, 'station_info.json')
        self.playlists_file    = os.path.join(self.station_dir, 'playlists.json')
        self.playlist_dir      = os.path.join(self.station_dir, 'playlists')
        self.parser            = Parser(self)
        # UGLY: it's not great that we are treating these attributes differently than REQD_CFG_ATTRS
        # (which are accessed implicitly through __getattr__()), but leave it this way for now!!!
        self.epoch             = self.config.get(ConfigKey.EPOCH)
        self.playlist_min      = self.config.get(ConfigKey.PLAYLIST_MIN)
        self.http_headers      = self.config.get(ConfigKey.HTTP_HEADERS, {})
        fetch_interval         = self.config.get(ConfigKey.FETCH_INTERVAL, DFLT_FETCH_INT)
        self.fetch_delta       = dt.timedelta(0, fetch_interval)
        self.sess              = requests.Session()
        self.synd_level        = self.config.get(ConfigKey.SYND_LEVEL)

        self.state = None
        self.playlists = None
        self.load_state()
        if self.status == Status.NEW:
            self.status = Status.ACTIVE if self.valid() else Status.INVALID

        self.last_fetch = dt.datetime.utcnow() - self.fetch_delta

    def __getattr__(self, key: str):
        try:
            return self.config[key]
        except KeyError:
            raise AttributeError()

    def station_info(self, keys: ObjCollect | str = INFO_KEYS, exclude: ObjCollect = None) -> dict:
        """Return station info (canonical fields) as a dict comprehension
        """
        stat = str(self.status)
        if not isinstance(keys, ObjCollect):
            keys = [keys]
        if isinstance(exclude, ObjCollect):
            keys = set(keys) - set(exclude)
        return {k: v for k, v in self.__dict__.items() if k in keys}

    def store_state(self):
        """Writes station info (canonical fields) to station_info.json file
        """
        with open(self.station_info_file, 'w') as f:
            json.dump(self.station_info(), f, indent=2)
        with open(self.playlists_file, 'w') as f:
            json.dump(self.playlists, f, indent=2)
        log.debug("Storing state for %s\n%s" %
                  (self.name, prettyprint(self.station_info(exclude=NOPRINT_KEYS), noprint=True)))

    def load_state(self, force = False):
        """Loads station info (canonical fields) from station_info.json file
        """
        if self.state is None or force:
            self.state = {}
            if os.path.isfile(self.station_info_file) and os.path.getsize(self.station_info_file) > 0:
                with open(self.station_info_file) as f:
                    station_info = json.load(f)
                self.state.update(station_info.get('state', {}))
        log.debug("Loading state for %s\n%s" %
                  (self.name, prettyprint(self.station_info(exclude=NOPRINT_KEYS), noprint=True)))

        if self.playlists is None or force:
            self.playlists = {}
            if os.path.isfile(self.playlists_file) and os.path.getsize(self.playlists_file) > 0:
                with open(self.playlists_file) as f:
                    playlists = json.load(f)
                self.playlists.update(playlists)
        log.debug("Loading playlists for %s" % self.name)

    def build_url(self, date):
        """Builds playlist URL based on url_fmt, which is a required attribute in the station info

        :param date: dt.date
        :return: string
        """
        url_fmt   = None
        tokens    = None
        date_func = None
        date_meth = None

        tz = ZoneInfo(self.timezone)
        today = dt.datetime.now(tz).date()

        for url_info in self.urls:
            cond = url_info[ConfigKey.COND]
            if re.fullmatch(r'(?:\+|\-)\d+', cond):
                cond_date = today + dt.timedelta(int(cond))
                if cond_date < today and (date < cond_date or date >= today):
                    continue
                elif cond_date >= today and (date < today or date > cond_date):
                    continue
            elif cond == DEFAULT_COND:
                if url_info is not self.urls[-1]:
                    raise RuntimeError("Default condition must be specified last")
            else:
                raise RuntimeError("Condition \"%s\" not recognized" % cond)

            url_fmt   = url_info[ConfigKey.URL_FMT]
            tokens    = re.findall(r'(\<[\p{Lu}\d_]+\>)', url_fmt)
            if not tokens:
                raise RuntimeError("No tokens in URL format string for cond \"%s\"" % cond)
            date_fmt  = url_info.get(ConfigKey.DATE_FMT)
            date_fmt2 = url_info.get(ConfigKey.DATE_FMT2)
            date_func = url_info.get(ConfigKey.DATE_FUNC)
            date_meth = url_info.get(ConfigKey.DATE_METH)
            break
        if not url_fmt:
            raise RuntimeError("No matching URL condition for date %s" % (date2str(date)))

        # note: "date_str" is a magic variable name that matches a URL format token
        date_str = date2str(date, date_fmt)
        if date_func:
            # note that date_func needs to be in the module namespace (e.g. imported)--by the
            # way, no real reason this here takes precedence over date_meth; we should really
            # either enforce mutual exclusion, or flip the two if we actually want to support
            # sequential execution (though no use case for that yet)
            date_str = globals()[date_func](date_str)
        elif date_meth:
            date_str = getattr(date_str, date_meth)()
        # hack to support additional date string (tied to "date_fmt2", if specified), works as
        # above, except without func/meth support (until we need it)
        if date_fmt2:
            date_str2 = date2str(date, date_fmt2)

        url = url_fmt
        for token in tokens:
            token_var = token[1:-1].lower()
            value = vars().get(token_var) or getattr(self, token_var, None)
            if not value:
                raise RuntimeError("Token attribute \"%s\" not found for \"%s\"" %
                                   (token_var, self.name))
            url = url.replace(token, value)

        return url

    def check(self, validate: bool = False):
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
            raise RuntimeError("Station \"%s\" already exists" % self.name)
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
        return date2str(date)

    def playlist_file(self, date):
        """Playlist file is playlist name plus data representation extension
        """
        filename = self.playlist_name(date) + '.' + self.playlist_ext
        return os.path.join(self.playlist_dir, filename)

    def get_playlists(self, ptrn: str = None) -> set:
        """Return names of playlists (from playlists directory)

        :param ptrn: glob pattern to match (optional trailing '*')
        :return: set iterator
        """
        if ptrn and ptrn[-1] != '*':
            ptrn += '*'
        files = glob.glob(os.path.join(self.playlist_dir, ptrn or '*'))
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
        all_dates = {date2str(dt.date.fromordinal(ord_)) for ord_ in ord_list}
        missing = all_dates.difference(fs_playlists)
        if len(missing) > 0:
            raise RuntimeError("Missing playlists for: " + str(sorted(missing)))

        # REVISIT: do we really want to just overwrite???
        self.state = {
            StateAttr.TOTAL    : len(self.playlists),
            StateAttr.EARLIEST : min(fs_playlists),
            StateAttr.LATEST   : max(fs_playlists),
            StateAttr.EPOCH    : self.epoch,
            # TEMP: None means we don't know!
            StateAttr.VALID    : None,
            StateAttr.MISSING  : None,
            StateAttr.INVALID  : None
        }
        log.debug("Validating playlist info for %s" % self.name)

        if not dryrun:
            self.store_state()

    def fetch_playlists(self, start_date, num = 1, dryrun = False, force = False):
        """Write specified playlists to filesystem
        """
        if isinstance(start_date, str):
            try:
                if start_date == FetchTarg.CATCHUP:
                    self.validate_playlists(dryrun=True)  # no need to store state yet
                    start_date = str2date(self.state.get(StateAttr.LATEST)) + dt.timedelta(1)
                    tz = ZoneInfo(self.timezone)
                    today = dt.datetime.now(tz).date()
                    num = (today - start_date).days
                    if num < 0:
                        raise RuntimeError("Latest playlist newer than yesterday")
                elif start_date == FetchTarg.MISSING:
                    raise ValueError("Not yet implemented")
                elif start_date == FetchTarg.INVALID:
                    raise ValueError("Not yet implemented")
                else:
                    start_date = str2date(start_date)
            except ValueError:
                raise RuntimeError("Invalid date to fetch")

        if num == 0:
            log.debug("No playlists to fetch for %s", self.name)
            return  # nothing to do

        # do this using ordinals instead of timedelta, since the math syntax is more native
        # (and hence logic more dependable)
        start_ord = start_date.toordinal()
        end_ord   = start_ord + num
        ord_step  = (1, -1)[num < 0]
        log.debug("Fetching %d playlist(s) starting with %s" % (num, date2str(start_date)))
        for ord_ in range(start_ord, end_ord, ord_step):
            date = dt.date.fromordinal(ord_)
            # TODO: should really create a Playlist here and encapsulate all of the fetch stuff!!!
            playlist_name = self.playlist_name(date)
            playlist_file = self.playlist_file(date)
            if os.path.exists(playlist_file) and os.path.getsize(playlist_file) > 0:
                if not force:
                    log.info("Skipping fetch for \"%s\", file exists" % playlist_name)
                    continue
                else:
                    # STUPID to call getsize() again, but keeps things cleaner above!!!
                    log.info("Forcing overwrite of existing file (size %d) for \"%s\"" %
                             (os.path.getsize(playlist_file), playlist_name))
            if self.epoch and date < str2date(self.epoch):
                if not force:
                    log.info("Skipping fetch for \"%s\", older than epoch \"%s\"" %
                             (playlist_name, self.epoch))
                    continue
                else:
                    log.info("Forcing fetch of \"%s\", older than epoch \"%s\"" %
                             (playlist_name, self.epoch))
            playlist_text = self.fetch_playlist(date)
            log.debug("Content for playlist \"%s\": %s..." % (playlist_name, playlist_text[:250]))
            if not dryrun:
                with open(playlist_file, 'w') as f:
                    f.write(playlist_text)
                status = PlaylistStatus.OK
                if self.playlist_min and len(playlist_text) < self.playlist_min:
                    log.info("Playlist \"%s\" content length %d below min" %
                             (playlist_name, len(playlist_text)))
                    status = PlaylistStatus.NOTOK
                self.playlists[playlist_name] = {
                    PlaylistAttr.FILE   : os.path.relpath(playlist_file, self.station_dir),
                    PlaylistAttr.SIZE   : len(playlist_text),
                    PlaylistAttr.STATUS : status
                }
            self.validate_playlists(dryrun)  # implicitly stores state, if not dryrun

    def fetch_playlist(self, date):
        """Fetch single playlist information (e.g. from internet)
        """
        # TODO: create context manager for this HTTP throttling mechanism!!!
        elapsed = dt.datetime.utcnow() - self.last_fetch
        if elapsed < self.fetch_delta:
            sleep_delta = self.fetch_delta - elapsed
            sleep(sleep_delta.seconds + sleep_delta.microseconds / 1000000.0)

        playlist_url = self.build_url(date)
        log.debug("Fetching from %s (headers: %s)" % (playlist_url, self.http_headers))
        r = self.sess.get(playlist_url, headers=self.http_headers)
        playlist_content = self.parser.proc_playlist(r.text)

        self.last_fetch = dt.datetime.utcnow()
        # TODO: this really needs to return metadata around the playlist, not
        # just the contents!!!
        return playlist_content


#####################
# command line tool #
#####################

import click

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
    elif cmd == 'create':
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
            station.fetch_playlists(date, num, dryrun, force)
    elif cmd == 'validate':
        for station_name in station_names:
            station = Station(station_name)
            station.validate_playlists(dryrun)

if __name__ == '__main__':
    main()

#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Station class/functions
"""

import os
import re
import json
import datetime as dt

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
REQUIRED_ATTRS = ['url_fmt', 'date_fmt']
STD_DATE_FMT   = '%Y-%m-%d'
DEBUG          = None

#################
# Station class #
#################

class Station(object):
    """
    """
    
    def __init__(self, name):
        """
        """
        self.name = name
        self.info = STATIONS.get(name)
        if not self.info:
            raise RuntimeError("Station \"%s\" not known" % (name))
        self.playlists = None

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

        # TEMP: just for initial dev/testing!!!
        self.todays_date = self.build_date(dt.date.today())
        self.todays_url = self.build_url(dt.date.today())

    def __getattr__(self, key):
        try:
            return self.info[key]
        except KeyError:
            raise AttributeError

    def build_date(self, date):
        """Builds date string based on date_fmt, which is a required attribute in the station info
        """
        return date.strftime(self.date_fmt)

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

    def debug(self):
        prettyprint(self.__dict__)
        
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
        valid = os.path.exists(self.playlist_dir) and os.path.isdir(self.playlist_dir)
        return valid

    def create(self):
        """Create station (raise exception if it already exists)
        """
        if self.check():
            raise RuntimeError("Station \"%s\" already exists" % (self.name))
        os.mkdir(self.station_dir)
        os.mkdir(self.playlist_dir)
        self.playlists = {}
        self.store_state()

    def store_state(self):
        with open(self.station_info_file, 'w') as f:
            json.dump(self.station_info(), f, indent=2)
        with open(self.playlists_file, 'w') as f:
            json.dump(self.playlists, f, indent=2)

    def station_info(self):
        return self.__dict__

    def fetch_playlist(self, date):
        """Fetch playlist information from internet
        """
        doc = {
            'name': self.playlist_name(date),
            'file': self.playlist_file(date)
        }
            
        return json.dumps(doc)

    def playlist_name(self, date):
        """
        """
        return date.strftime(STD_DATE_FMT)

    def playlist_file(self, date):
        """
        """
        filename = self.playlist_name(date) + '.json'
        return os.path.join(self.playlist_dir, filename)

    def store_playlists(self, start_date, num=1, **flags):
        """Write specified playlists to filesystem
        """
        # FIX: this doesn't really belong here, should go in station load function!!!
        if self.playlists is None:
            with open(self.playlists_file) as f:
                self.playlists = json.load(f)

        start_ord = start_date.toordinal()
        end_ord   = start_ord + num
        ord_step  = (1, -1)[num < 0]
        for ord in range(start_ord, end_ord, ord_step):
            date = dt.date.fromordinal(ord)
            playlist_name = self.playlist_name(date)
            playlist_text = self.fetch_playlist(date)
            if DEBUG:
                prettyprint(playlist_text)
            filename = self.playlist_file(date)
            with open(filename, 'w') as f:
                f.write(playlist_text)
            self.playlists[playlist_name] = {'file': filename, 'status': 'ok'}

        self.store_state()

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
@click.option('--debug',     default=0, help="Debug level")
@click.argument('name',      default='all', required=True)
def main(cmd, date, num, skip, force, debug, name):
    """Manage station information for comma-separated list of station names (or 'all')
    """
    global DEBUG

    DEBUG = debug
    if name == 'all':
        station_names = STATIONS.keys()
    else:
        station_names = name.upper().split(',')

    if cmd == 'create':
        for station_name in station_names:
            station = Station(station_name)
            if DEBUG > 0:
                station.debug()
            station.create()

    if cmd == 'fetch':
        try:
            start_date = dt.datetime.strptime(date, '%Y-%m-%d').date()
        except ValueError:
            raise RuntimeError("Invalid date to fetch")

        for station_name in station_names:
            station = Station(station_name)
            station.store_playlists(start_date, num)

if __name__ == '__main__':
    main()

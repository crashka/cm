#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Station class/functions
"""

import os
import re
import json
from datetime import date

import yaml
import click
import requests

from utils import prettyprint

################
# config stuff #
################

FILE_DIR     = os.path.dirname(os.path.realpath(__file__))
BASE_DIR     = os.path.join(FILE_DIR, os.pardir)
CONFIG_FILE  = "config/config.yml"
CONFIG_PATH  = os.path.join(BASE_DIR, CONFIG_FILE)
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

STATIONS = config('stations')
REQUIRED_ATTRS = ['url_fmt', 'date_fmt']

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

        for attr in REQUIRED_ATTRS:
            if attr not in self.info:
                raise RuntimeError("Required config attribute \"%s\" missing for \"%s\"" % (attr, name))

        # extract tokens in url_fmt upfront
        self.tokens = re.findall(r'(\<[A-Z_]+\>)', self.url_fmt)
        if not self.tokens:
            raise RuntimeError("No tokens in URL format string for \"%s\"" % (name))

        # TEMP: just for initial dev/testing!!!
        self.todays_date = self.build_date(date.today())
        self.todays_url = self.build_url(date.today())

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
        pass

    def create(self):
        """Create station (raise exception if it already exists)
        """
        pass

    def get_playlist(date):
        """Get playlist from internet
        """
        pass

    def store_playlists(start_date, end_date, **flags):
        """Write specified playlists to filesystem
        """
        pass

#####################
# command line tool #
#####################

@click.command()
@click.option('--create/--no-create', default=False, help="create specified stations if they don't exist (default: --no-create)")
@click.option('--name', default='all', help="comma-separated list of station names, or 'all' (default: 'all')")
@click.option('--debug', default=0, help="debug level (default: 0)")
def main(create, name, debug):
    """Command line tool for managing station information
    """
    if name == 'all':
        station_names = STATIONS.keys()
    else:
        station_names = name.upper().split(',')

    if create:
        for station_name in station_names:
            station = Station(station_name)
            if debug > 0:
                station.debug()

if __name__ == '__main__':
    main()

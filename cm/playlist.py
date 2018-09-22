#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Playlist module
"""

from __future__ import absolute_import, division, print_function

import os.path
import logging

import click

import station
from utils import LOV, prettyprint, str2date, date2str, strtype, collecttype

##############################
# common constants/functions #
##############################

INFO_KEYS    = set(['sta_name',
                    'datestr',
                    'name',
                    'status',
                    'file'])
NOPRINT_KEYS = set([])

# Lists of Values
Status = LOV(['NEW',
              'MISSING',
              'VALID',
              'INVALID',
              'DISABLED'], 'lower')

# shared resources from station
cfg      = station.cfg
log      = station.log
sess     = station.sess
dbg_hand = station.dbg_hand

##################
# Playlist class #
##################

class Playlist(object):
    """Represents a playlist for a station
    """
    PARSER_MAP = {
        'json': 'parse_json',
        'html': 'parse_html'
    }

    @staticmethod
    def list(sta):
        """List playlists for a station

        :param sta: station object
        :return: sorted list of playlist names (same as date)
        """
        return sorted(sta.get_playlists())

    def __init__(self, sta, date):
        """Sets status field locally (but not written back to info file)

        :param sta: station object
        "param date: dt.date (or Y-m-d string)
        """
        self.station  = sta
        self.sta_name = sta.name
        self.date     = str2date(date) if strtype(date) else date
        self.datestr  = date2str(self.date)
        log.debug("Instantiating Playlist(%s, %s)" % (sta.name, self.datestr))
        self.name     = sta.playlist_name(self.date)
        self.file     = sta.playlist_file(self.date)
        self.status   = Status.NEW

    def playlist_info(self, keys = INFO_KEYS, exclude = None):
        """Return station info (canonical fields) as a dict comprehension
        """
        if not collecttype(keys):
            keys = [keys]
        if collecttype(exclude):
            keys = set(keys) - set(exclude)
        return {k: v for k, v in self.__dict__.items() if k in keys}

    def parse(self):
        """Return station info (canonical fields) as a dict comprehension
        """
        format = self.station.playlist_ext
        parser_name = Playlist.PARSER_MAP.get(format)
        if not parser_name:
            raise RuntimeError("playlist format %s not known" % (format))
        return getattr(Playlist, parser_name)(self)

    def parse_json(self):
        """Return station info (canonical fields) as a dict comprehension
        """
        log.debug("parsing json")

    def parse_html(self):
        """Return station info (canonical fields) as a dict comprehension
        """
        log.debug("parsing html")

#####################
# command line tool #
#####################

@click.command()
@click.option('--list',      'cmd', flag_value='list', default=True, help="List all (or specified) playlists")
#@click.option('--create',    'cmd', flag_value='create', help="Create new playlist (skip if playlist exists)")
@click.option('--parse',     'cmd', flag_value='parse', help="Parse out play information from playlist")
#@click.option('--fetch',     'cmd', flag_value='fetch', help="Fetch playlists for playlist (fail if playlist does not exist)")
@click.option('--validate',  'cmd', flag_value='validate', help="Validate playlist file contents")
@click.option('--station',   'sta_name', help="Station (name) for playlist", required=True)
#@click.option('--skip',      is_flag=True, help="Skip (rather than fail) if playlist does not exist")
#@click.option('--force',     is_flag=True, help="Overwrite existing playlists (otherwise skip over), applies only to --fetch")
@click.option('--dryrun',    is_flag=True, help="Do not execute write, log to INFO level instead")
@click.option('--debug',     default=0, help="Debug level")
@click.argument('playlists', default='all', required=True)
def main(cmd, sta_name, dryrun, debug, playlists):
    """Manage playlist information for playlists within a station
    
    Currently, playlist date (or 'all') is specified.

    Later, be able to parse out range of playlists.
    """
    if debug > 0:
        log.setLevel(logging.DEBUG)
        log.addHandler(dbg_hand)

    sta = station.Station(sta_name.upper())

    if playlists == 'all':
        playlist_names = Playlist.list(sta)
    else:
        playlist_names = playlists.split(',')

    if cmd == 'list':
        for playlist_name in playlist_names:
            playlist = Playlist(sta, playlist_name)
            prettyprint(playlist.playlist_info(exclude=NOPRINT_KEYS))
    elif cmd == 'parse':
        for playlist_name in playlist_names:
            playlist = Playlist(sta, playlist_name)
            playlist.parse()
    elif cmd == 'validate':
        raise RuntimeError("Not yet implemented")

if __name__ == '__main__':
    main()

#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Playlist module
"""

import datetime as dt
from os.path import relpath
import logging

from .utils import LOV, prettyprint, str2date, date2str
from .core import log, dbg_hand, ObjCollect
from .datasci import HashSeq

##############################
# common constants/functions #
##############################

INFO_KEYS    = ('sta_name',
                'datestr',
                'name',
                'status',
                'file',
                'parsed_info')
NOPRINT_KEYS = ('parsed_info', )

# Lists of Values
PLStatus = LOV(['NEW',
                'PARSED'], 'lower')

##################
# Playlist class #
##################

class Playlist:
    """Represents a playlist for a station
    """
    @staticmethod
    def list(sta: 'Station', ptrn: str = None) -> list:
        """List playlists for a station

        :param sta: station object
        :param ptrn: glob pattern to match (optional trailing '*')
        :return: sorted list of playlist names (same as date)
        """
        return sorted(sta.get_playlists(ptrn))

    def __init__(self, sta: 'Station', date: dt.date | str):
        """Sets status field locally (but not written back to info file)

        :param sta: station object
        "param date: dt.date (or Y-m-d string)
        """
        self.station     = sta
        self.sta_name    = sta.name
        self.parser      = sta.parser
        self.date        = str2date(date) if isinstance(date, str) else date
        self.datestr     = date2str(self.date)
        log.debug("Instantiating Playlist(%s, %s)" % (sta.name, self.datestr))
        self.name        = sta.playlist_name(self.date)
        self.file        = sta.playlist_file(self.date)
        self.rel_path    = relpath(self.file, self.station.station_dir)
        self.status      = PLStatus.NEW
        # TODO: preload trailing hash sequence from previous playlist (or add
        # task to fill the gap as to_do_list item)!!!
        self.hash_seq    = HashSeq()
        self.parse_ctx   = {}
        self.parsed_info = None

    def playlist_info(self, keys = INFO_KEYS, exclude = None) -> dict:
        """Return playlist info (canonical fields) as a dict comprehension
        """
        if not isinstance(keys, ObjCollect):
            keys = [keys]
        if isinstance(exclude, ObjCollect):
            keys = set(keys) - set(exclude)
        return {k: v for k, v in self.__dict__.items() if k in keys}

    def parse(self, dryrun = False, force = False) -> dict:
        """Parse current playlist using underlying parser

        :param dryrun: don't write to database
        :param force: overwrite program_play/play in databsae
        :return: dict with parsed program_play/play info
        """
        if self.parsed_info:
            # LATER: overwrite existing parse information if force=True!!!
            raise RuntimeError("Playlist already parsed (force not yet implemented)")
        self.parsed_info = self.parser.parse(self, dryrun, force)
        self.status = PLStatus.PARSED
        return self.parsed_info

    def analyze(self, dryrun = False, force = False) -> None:
        """Analyze current playlist using underlying parser

        :param dryrun: don't write to config file
        :param force: overwrite playlist configuration, if exists
        :return: void
        """
        self.parser.analyze(self, dryrun, force)

#####################
# command line tool #
#####################

import click
from . import station

@click.command()
@click.option('--list',      'cmd', flag_value='list', default=True, help="List all (or specified) playlists")
@click.option('--parse',     'cmd', flag_value='parse', help="Parse out play information from playlist")
@click.option('--analyze',   'cmd', flag_value='analyze', help="Analyze play information from playlist")
@click.option('--validate',  'cmd', flag_value='validate', help="Validate playlist file contents")
@click.option('--station',   'sta_name', help="Station (name) for playlist", required=True)
#@click.option('--skip',      is_flag=True, help="Skip (rather than fail) if playlist does not exist")
@click.option('--force',     is_flag=True, help="Overwrite existing playlist info, applies only to --parse")
@click.option('--dryrun',    is_flag=True, help="Do not execute write, log to INFO level instead")
@click.option('--debug',     default=0, help="Debug level")
@click.argument('playlists', default='all', required=True)
def main(cmd, sta_name, force, dryrun, debug, playlists):
    """Manage playlist information for playlists within a station

    Playlists may be specified as comma-separated list of dates, glob pattern (with
    optional trailing '*'), or 'all'.
    """
    if debug > 0:
        log.setLevel(logging.DEBUG)
        log.addHandler(dbg_hand)

    sta = station.Station(sta_name.upper())

    if playlists == 'all':
        playlist_names = Playlist.list(sta)
    elif ',' in playlists:
        playlist_names = playlists.split(',')
    else:
        playlist_names = Playlist.list(sta, playlists)

    if cmd == 'list':
        for playlist_name in playlist_names:
            playlist = Playlist(sta, playlist_name)
            prettyprint(playlist.playlist_info(exclude=NOPRINT_KEYS))
    elif cmd == 'parse':
        for playlist_name in playlist_names:
            playlist = Playlist(sta, playlist_name)
            playlist.parse()
    elif cmd == 'analyze':
        for playlist_name in playlist_names:
            playlist = Playlist(sta, playlist_name)
            playlist.analyze()
    elif cmd == 'validate':
        raise RuntimeError("Not yet implemented")

if __name__ == '__main__':
    main()

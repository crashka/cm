#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Playlist module
"""

from __future__ import absolute_import, division, print_function

import os.path
import logging
import json

import click

import station
import musiclib
from datasci import HashSeq
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
cfg       = station.cfg
log       = station.log
sess      = station.sess
dflt_hand = station.dflt_hand
dbg_hand  = station.dbg_hand

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
        # TODO: preload trailing hash sequence from previous playlist (or add
        # task to fill the gap as to_do_list item)!!!
        self.hash_seq = HashSeq()

    def playlist_info(self, keys = INFO_KEYS, exclude = None):
        """Return station info (canonical fields) as a dict comprehension
        """
        if not collecttype(keys):
            keys = [keys]
        if collecttype(exclude):
            keys = set(keys) - set(exclude)
        return {k: v for k, v in self.__dict__.items() if k in keys}

    def parse(self, dryrun = False, force = False):
        """Return station info (canonical fields) as a dict comprehension
        """
        format = self.station.playlist_ext
        parser_name = Playlist.PARSER_MAP.get(format)
        if not parser_name:
            raise RuntimeError("playlist format %s not known" % (format))
        return getattr(Playlist, parser_name)(self, dryrun, force)

    def parse_json(self, dryrun = False, force = False):
        """Return station info (canonical fields) as a dict comprehension
        """
        log.debug("Parsing json for %s", os.path.relpath(self.file, self.station.station_dir))
        with open(self.file) as f:
            pl_info = json.load(f)
        pl_params = pl_info['params']
        pl_progs = pl_info['onToday']

        for prog in pl_progs:
            assert isinstance(prog, dict)
            prog_info = prog.get('program')
            assert isinstance(prog_info, dict)

            # TEMP: get rid of dev/debugging stuff (careful, prog_name used below)!!!
            prog_desc = prog_info.get('program_desc')
            prog_name = prog_info.get('name') + (' - ' + prog_desc if prog_desc else '')
            log.debug("PROGRAM [%s]: %s" % (prog['fullstart'], prog_name))
            prog_copy = prog.copy()
            del prog_copy['playlist']
            log.debug(prettyprint(prog_copy, noprint=True))

            pp_rec = musiclib.insert_program_play(self.station, prog)
            if not pp_rec:
                raise RuntimeError("Could not insert program play")
            else:
                log.debug("Created program play ID %d" % (pp_rec['id']))

            plays = prog.get('playlist')
            if plays:
                for play in plays:
                    assert isinstance(play, dict)

                    play_name = "%s - %s" % (play.get('composerName'), play.get('trackName'))
                    if play.get('_start_time'):
                        log.debug("PLAY [%s]: %s" % (play.get('_start_time'), play_name))
                        log.debug(prettyprint(play, noprint=True))
                    else:
                        log.debug("PLAY: %s" % (play_name))
                        log.debug(prettyprint(play, noprint=True))

                    play_rec = musiclib.insert_play(self.station, pp_rec, play)
                    if not play_rec:
                        raise RuntimeError("Could not insert play")
                    else:
                        log.debug("Created play ID %d" % (play_rec['id']))

                    # TODO: create separate hash sequence for top of each hour!!!
                    play_seq = self.hash_seq.add(play_name)
                    #log.debug('Hash seq: ' + str(play_seq))
            else:
                log.debug("No plays for program \"%s\"" % (prog_name))

    def parse_html(self, dryrun = False, force = False):
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
@click.option('--force',     is_flag=True, help="Overwrite existing playlist info, applies only to --parse")
@click.option('--dryrun',    is_flag=True, help="Do not execute write, log to INFO level instead")
@click.option('--debug',     default=0, help="Debug level")
@click.argument('playlists', default='all', required=True)
def main(cmd, sta_name, force, dryrun, debug, playlists):
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

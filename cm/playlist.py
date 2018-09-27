#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Playlist module
"""

from __future__ import absolute_import, division, print_function

import os.path
import logging
import json
import datetime as dt

import click

import core
import station
from musiclib import get_handle
from datasci import HashSeq
from utils import LOV, prettyprint, str2date, date2str, str2time, strtype, collecttype

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

##############################
# common constants/functions #
##############################

INFO_KEYS    = set(['sta_name',
                    'datestr',
                    'name',
                    'status',
                    'file',
                    'parsed_info'])
NOPRINT_KEYS = set(['parsed_info'])

# Lists of Values
Status       = LOV(['NEW',
                    'PARSED'], 'lower')

Name         = LOV({'NONE'   : '<none>',
                    'UNKNOWN': '<unknown>'})

##################
# Playlist class #
##################

class Playlist(object):
    """Represents a playlist for a station
    """
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
        self.station     = sta
        self.sta_name    = sta.name
        self.parser      = sta.parser
        self.date        = str2date(date) if strtype(date) else date
        self.datestr     = date2str(self.date)
        log.debug("Instantiating Playlist(%s, %s)" % (sta.name, self.datestr))
        self.name        = sta.playlist_name(self.date)
        self.file        = sta.playlist_file(self.date)
        self.status      = Status.NEW
        # TODO: preload trailing hash sequence from previous playlist (or add
        # task to fill the gap as to_do_list item)!!!
        self.hash_seq    = HashSeq()
        self.parsed_info = None

    def playlist_info(self, keys = INFO_KEYS, exclude = None):
        """Return playlist info (canonical fields) as a dict comprehension
        """
        if not collecttype(keys):
            keys = [keys]
        if collecttype(exclude):
            keys = set(keys) - set(exclude)
        return {k: v for k, v in self.__dict__.items() if k in keys}

    def parse(self, dryrun = False, force = False):
        """Parse current playlist using underlying parser

        :param dryrun: don't write to database
        :param force: overwrite program_play/play in databsae
        :return: dict with parsed program_play/play info
        """
        if self.parsed_info:
            # LATER: overwrite existing parse information if force=True!!!
            raise RuntimeError("Playlist already parsed (force not yet implemented)")
        self.parsed_info = self.parser.parse(self, dryrun, force)
        self.status = Status.PARSED
        return self.parsed_info

##################
# Parser classes #
##################

def get_parser(cls_name):
    cls = globals().get(cls_name)
    return cls() if cls else None

# TODO: move to subdirectory(ies) when this gets too unwieldy!!!

class Parser(object):
    """Basically a helper class for Playlist, associated through station config

    Note: could (should???) implement all methods as static
    """
    def __init__(self):
        """Parser object is stateless, so constructor doesn't do anything
        """
        pass

    def parse(self, playlist, dryrun = False, force = False):
        """Abstract method for parse operation
        """
        raise RuntimeError("parse() not implemented in subclass (or base class called)")

    def insert_program_play(self, playlist, data):
        """
        :param playlist: parent Playlist object
        :param data: normalized playlist key/value data (dict)
        :return: key-value dict comprehension for inserted program_play fields
        """
        station = playlist.station

        sta = get_handle('station')
        sta_res = sta.select({'name': station.name})
        if sta_res.rowcount == 1:
            sta_row = sta_res.fetchone()
        else:
            log.debug("Inserting station \"%s\" into musiclib" % (station.name))
            ins_res = sta.insert({'name': station.name, 'timezone': station.time_zone})
            if ins_res.rowcount == 0:
                raise RuntimeError("Could not insert station \"%s\" into musiclib" % (station.name))
            sta_row = sta.inserted_row(ins_res)
            if not sta_row:
                raise RuntimeError("Station %s not in musiclib" % (station.name))

        prog_data = data['program']
        prog_name = prog_data['name']
        prog = get_handle('program')
        prog_res = prog.select({'name': prog_name})
        if prog_res.rowcount == 1:
            prog_row = prog_res.fetchone()
        else:
            log.debug("Inserting program \"%s\" into musiclib" % (prog_name))
            ins_res = prog.insert(data['program'])
            if ins_res.rowcount == 0:
                raise RuntimeError("Could not insert program \"%s\" into musiclib" % (prog_name))
            prog_row = prog.inserted_row(ins_res)
            if not prog_row:
                raise RuntimeError("Program \"%s\" not in musiclib" % (prog_name))

        pp_data = data['program_play']
        pp_data['station_id'] = sta_row.id
        pp_data['program_id'] = prog_row.id
        prog_play = get_handle('program_play')
        ins_res = prog_play.insert(pp_data)
        pp_row = prog_play.inserted_row(ins_res)
        return {k: v for k, v in pp_row.items()} if pp_row else None

    def insert_play(self, playlist, prog_play, data):
        """
        :param playlist: parent Playlist object
        :param prog_play: parent program_play fields (dict)
        :param data: normalized play key/value data (dict)
        :return: key-value dict comprehension for inserted play fields
        """
        station = playlist.station

        sta = get_handle('station')
        sta_res = sta.select({'name': station.name})
        if sta_res.rowcount == 1:
            sta_row = sta_res.fetchone()
        else:
            log.debug("Inserting station \"%s\" into musiclib" % (station.name))
            ins_res = sta.insert({'name': station.name, 'timezone': station.time_zone})
            if ins_res.rowcount == 0:
                raise RuntimeError("Could not insert station \"%s\" into musiclib" % (station.name))
            sta_row = sta.inserted_row(ins_res)
            if not sta_row:
                raise RuntimeError("Station %s not in musiclib" % (station.name))

        comp_data = data['composer']
        # NOTE: we always make sure there is a composer record (even if NONE or UNKNOWN), since work depends
        # on it (and there is no play without work, haha)
        if not comp_data['name']:
            comp_data['name'] = Name.NONE
        comp_name = comp_data['name']
        comp = get_handle('person')
        comp_res = comp.select({'name': comp_name})
        if comp_res.rowcount == 1:
            comp_row = comp_res.fetchone()
        else:
            log.debug("Inserting composer \"%s\" into musiclib" % (comp_name))
            ins_res = comp.insert(comp_data)
            if ins_res.rowcount == 0:
                raise RuntimeError("Could not insert composer/person \"%s\" into musiclib" % (comp_name))
            comp_row = comp.inserted_row(ins_res)
            if not comp_row:
                raise RuntimeError("Composer/person \"%s\" not in musiclib" % (comp_name))

        work_data = data['work']
        if not work_data['name']:
            log.debug("Work name not specified, skipping...")
            return None
        work_name = work_data['name']
        work_data['composer_id'] = comp_row.id
        work = get_handle('work')
        work_res = work.select({'name': work_name, 'composer_id': comp_row.id})
        if work_res.rowcount == 1:
            work_row = work_res.fetchone()
        else:
            log.debug("Inserting work \"%s\" into musiclib" % (work_name))
            ins_res = work.insert(work_data)
            if ins_res.rowcount == 0:
                raise RuntimeError("Could not insert work/person \"%s\" into musiclib" % (work_name))
            work_row = work.inserted_row(ins_res)
            if not work_row:
                raise RuntimeError("Work/person \"%s\" not in musiclib" % (work_name))

        cond_row = None
        cond_data = data['conductor']
        cond_name = cond_data['name']
        if cond_name:
            cond = get_handle('person')
            cond_res = cond.select({'name': cond_name})
            if cond_res.rowcount == 1:
                cond_row = cond_res.fetchone()
            else:
                log.debug("Inserting conductor \"%s\" into musiclib" % (cond_name))
                ins_res = cond.insert(cond_data)
                if ins_res.rowcount == 0:
                    raise RuntimeError("Could not insert conductor/person \"%s\" into musiclib" % (cond_name))
                cond_row = cond.inserted_row(ins_res)
                if not cond_row:
                    raise RuntimeError("Conductor/person \"%s\" not in musiclib" % (cond_name))

        play_data = data['play']
        play_data['station_id']   = sta_row.id
        play_data['prog_play_id'] = prog_play['id']
        play_data['program_id']   = prog_play['program_id']
        play_data['composer_id']  = comp_row.id
        play_data['work_id']      = work_row.id
        play_data['conductor_id'] = cond_row.id if cond_row else None
        play = get_handle('play')
        ins_res = play.insert(play_data)
        play_row = play.inserted_row(ins_res)
        return {k: v for k, v in play_row.items()} if play_row else None

class ParserWWFM(Parser):
    """Represents a playlist for a station
    """
    def parse(self, playlist, dryrun = False, force = False):
        """Parse playlist, write to musiclib if not dryrun

        :param playlist: Playlist object
        :param dryrun: don't write to database
        :param force: overwrite program_play/play in databsae
        :return: dict with parsed program_play/play info
        """
        log.debug("Parsing json for %s", os.path.relpath(playlist.file, playlist.station.station_dir))
        with open(playlist.file) as f:
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

            norm = self.map_program_play(prog)
            pp_rec = self.insert_program_play(playlist, norm)
            if not pp_rec:
                raise RuntimeError("Could not insert program play")
            else:
                log.debug("Created program play ID %d" % (pp_rec['id']))
            pp_rec['plays'] = []

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

                    norm = self.map_play(play)
                    play_rec = self.insert_play(playlist, pp_rec, norm)
                    if not play_rec:
                        raise RuntimeError("Could not insert play")
                    else:
                        log.debug("Created play ID %d" % (play_rec['id']))
                    pp_rec['plays'].append(play_rec)

                    # TODO: create separate hash sequence for top of each hour!!!
                    play_seq = playlist.hash_seq.add(play_name)
                    #log.debug('Hash seq: ' + str(play_seq))
            else:
                log.debug("No plays for program \"%s\"" % (prog_name))
        return pp_rec

    def map_program_play(self, data):
        """This is the version for WWFM

        raw data in: 'onToday' item from WWFM playlist file
        normalized data out: {
            'program': {},
            'program_play': {}
        }
        """
        prog_info = data['program']
        prog_name = prog_info['name']
        prog_data = {'name': prog_name}

        data.get('date')        # 2018-09-19
        data.get('fullstart')   # 2018-09-19 12:00
        data.get('start_time')  # 12:00
        data.get('start_utc')   # Wed Sep 19 2018 12:00:00 GMT-0400 (EDT)
        data.get('fullend')     # 2018-09-19 13:00
        data.get('end_time')    # 13:00
        data.get('end_utc')     # Wed Sep 19 2018 13:00:00 GMT-0400 (EDT)

        sdate, stime = data['fullstart'].split()
        edate, etime = data['fullend'].split()
        if sdate != data['date']:
            log.debug("Date mismatch %s != %s" % (sdate, data['date']))
        if stime != data['start_time']:
            log.debug("Start time mismatch %s != %s" % (stime, data['start_time']))
        if etime != data['end_time']:
            log.debug("End time mismatch %s != %s" % (etime, data['end_time']))

        pp_data = {}
        pp_data['prog_play_info'] =  data
        pp_data['prog_play_date'] =  str2date(sdate)
        pp_data['prog_play_start'] = str2time(stime)
        pp_data['prog_play_end'] =   str2time(etime)
        pp_data['prog_play_dur'] =   None # Interval, if listed
        pp_data['notes'] =           None # ARRAY(Text)),
        pp_data['start_time'] =      None # TIMESTAMP(timezone=True)),
        pp_data['end_time'] =        None # TIMESTAMP(timezone=True)),
        pp_data['duration'] =        None # Interval)

        return {'program': prog_data, 'program_play': pp_data}

    def map_play(self, data):
        """This is the version for WWFM

        raw data in: 'playlist' item from WWFM playlist file
        normalized data out: {
            'composer'  : {},
            'work'      : {},
            'conductor' : {},
            'performers': [{}, ...],
            'ensembles' : [{}, ...],
            'recording' : {},
            'play'      : {}
        }
        """
        # do the easy stuff first (don't worry about empty records for now)
        composer_data  =  {'name'      : data.get('composerName')}
        work_data      =  {'name'      : data.get('trackName')}
        conductor_data =  {'name'      : data.get('conductor')}
        recording_data =  {'label'     : data.get('copyright'),
                           'catalog_no': data.get('catalogNumber'),
                           'name'      : data.get('collectionName')}

        # for performers, combine 'artistName' and 'soloists' (note that both/either can be
        # comma- or semi-colon-delimited)
        performers_data = []
        for perf_str in (data.get('artistName'), data.get('soloists')):
            if not perf_str:
                continue
            if ';' in perf_str:
                perfs = perf_str.split(';')
                for perf in perfs:
                    fields = perf.rsplit(',', 1)
                    if len(fields) == 1:
                        performers_data.append({'name': fields[0], 'role': None})
                    else:
                        performers_data.append({'name': fields[0].strip(), 'role': fields[1].strip()})
            elif perf_str.count(',') % 2 == 1:
                fields = perf_str.split(',')
                while fields:
                    pers, role = (fields.pop(0), fields.pop(0))
                    performers_data.append({'name': pers.strip(), 'role': role.strip()})
            else:
                # TODO: if even number of commas, need to look closer at string contents/format
                # to figure out what to do!!!
                performers_data.append({'name': perf_str, 'role': None})

        # treat ensembles similar to performers, except no need to parse within semi-colon-delimited
        # fields, and slightly different logic for comma-delimited fields
        ensembles_data  =  []
        ensembles_str = data.get('ensembles')
        if ensembles_str:
            if ';' in ensembles_str:
                ensembles = ensembles_str.split(';')
                ensembles_data += [{'name': ens.strip()} for ens in ensembles]
            elif ',' in ensembles_str:
                fields = ensembles_str.split(',')
                while fields:
                    if len(fields) == 1:
                        ensembles_data.append({'name': fields.pop(0).strip()})
                        break  # same as continue
                    # more reliable to do this moving backward from the end (sez me)
                    if ' ' not in fields[-1]:
                        # REVISIT: we presume a single-word field to be a city/location (for now);
                        # as above, we should really look at field contents to properly parse!!!
                        ens = ','.join([fields.pop(-2), fields.pop(-1)])
                        ensembles_data.append({'name': ens.strip()})
                    else:
                        # yes, do this twice!
                        ensembles_data.append({'name': fields.pop(-1).strip()})
                        ensembles_data.append({'name': fields.pop(-1).strip()})
            else:
                ensembles_data.append({'name': ensembles_str})

        data.get('_id')              # 5b997ff162a4197540403ef5

        data.get('_date')            # 09202018
        data.get('_start')           # 02:10:20
        data.get('_start_time')      # 09-20-2018 03:10:20
        data.get('_start_datetime')  # 2018-09-20T06:10:20.000Z
        data.get('_end')             #
        data.get('_end_time')        # 09-20-2018 03:39:42
        data.get('_end_datetime')    # 2018-09-20T06:39:42.000Z
        data.get('_duration')        # 1762000 [msecs]

        data.get('composerName')     # Mauro Giuliani
        data.get('trackName')        # Guitar Concerto No. 3
        data.get('ensembles')        # Academy of St Martin in the Fields
        data.get('soloists')         #
        data.get('instruments')      # OXx
        data.get('artistName')       # Pepe Romero, guitar
        data.get('conductor')        # Neville Marriner

        data.get('copyright')        # Philips
        data.get('catalogNumber')    # 420780
        data.get('trackNumber')      # 4-6
        data.get('collectionName')   #
        data.get('releaseDate')      #
        data.get('upc')              #
        data.get('imageURL')         #
        data.get('program')          #
        data.get('episode_notes')    #
        data.get('_err')             # []

        sdate, stime = data['_start_time'].split()
        if '_end_time' in data:
            edate, etime = data['_end_time'].split()
        else:
            edate, etime = (None, None)

        # NOTE: would like to do integrity check, but need to rectify formatting difference
        # for date, hour offset for time, non-empty value for _end!!!
        #if sdate != data['_date']:
        #    log.debug("Date mismatch %s != %s" % (sdate, data['date']))
        #if stime != data['_start']:
        #    log.debug("Start time mismatch %s != %s" % (stime, data['start_time']))
        #if etime != data['_end']:
        #    log.debug("End time mismatch %s != %s" % (etime, data['end_time']))

        dur_msecs = data.get('_duration')

        play_data = {}
        play_data['play_info'] =  data
        play_data['play_date'] =  str2date(sdate, '%m-%d-%Y')
        play_data['play_start'] = str2time(stime)
        play_data['play_end'] =   str2time(etime) if etime else None
        play_data['play_dur'] =   dt.timedelta(0, 0, 0, dur_msecs) if dur_msecs else None
        play_data['notes'] =      None # ARRAY(Text)),
        play_data['start_time'] = None # TIMESTAMP(timezone=True)),
        play_data['end_time'] =   None # TIMESTAMP(timezone=True)),
        play_data['duration'] =   None # Interval)

        return {
            'composer':   composer_data,
            'work':       work_data,
            'conductor':  conductor_data,
            'performers': performers_data,
            'ensembles':  ensembles_data,
            'recording':  recording_data,
            'play':       play_data
        }


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

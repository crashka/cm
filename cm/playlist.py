#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Playlist module
"""

from __future__ import absolute_import, division, print_function

import os.path
import logging
import json
import regex as re
import datetime as dt
from urlparse import urlsplit, parse_qs

import pytz
from bs4 import BeautifulSoup

import core
from musiclib import (MusicLib, COND_STRS, SKIP_ENS, ml_dict, parse_composer_str, parse_work_str,
                      parse_conductor_str, parse_performer_str, parse_ensemble_str)
from datasci import HashSeq
from utils import (LOV, prettyprint, str2date, date2str, str2time, time2str, datetimetz,
                   strtype, collecttype)

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
PLStatus = LOV(['NEW',
                'PARSED'], 'lower')

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
        self.status      = PLStatus.NEW
        # TODO: preload trailing hash sequence from previous playlist (or add
        # task to fill the gap as to_do_list item)!!!
        self.hash_seq    = HashSeq()
        self.parse_ctx   = {}
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
        self.status = PLStatus.PARSED
        return self.parsed_info

#####################
# Parser base class #
#####################

class Parser(object):
    """Basically a helper class for Playlist, proper subclass is associated through
    station config

    FIX: cleanup object relationships between Station, Playlist, and Parser!!!
    """
    @staticmethod
    def get(sta):
        """
        :param sta: parser is bound to Station instance
        :return: instantiated Parser subclass
        """
        cls = globals().get(sta.parser_cls)
        return cls(sta) if cls else None

    def __init__(self, sta):
        """Parser object is stateless, other than Station backreference, so constructor
        doesn't really do anything
        """
        self.station = sta

    def parse(self, playlist, dryrun = False, force = False):
        """Abstract method for parse operation
        """
        raise RuntimeError("parse() not implemented in subclass (or base class called)")

##########################
# Parser adapter classes #
##########################

# TODO: move to subdirectory(ies) when this gets too unwieldy!!!

#+------------+
#| ParserWWFM |
#+------------+

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
            # Step 1 - Parse out program_play info
            norm = self.map_program_play(prog)
            pp_rec = MusicLib.insert_program_play(playlist, norm)
            if not pp_rec:
                raise RuntimeError("Could not insert program_play")
            playlist.parse_ctx['station_id']   = pp_rec['station_id']
            playlist.parse_ctx['prog_play_id'] = pp_rec['id']
            playlist.parse_ctx['play_id']      = None
            pp_rec['plays'] = []

            # Step 2 - Parse out play info (if present)
            plays = prog.get('playlist') or []
            for play in plays:
                norm = self.map_play(pp_rec, play)
                play_rec = MusicLib.insert_play(playlist, pp_rec, norm)
                if not play_rec:
                    raise RuntimeError("Could not insert play")
                playlist.parse_ctx['play_id'] = play_rec['id']
                pp_rec['plays'].append(play_rec)

                es_recs = MusicLib.insert_entity_strings(playlist, norm)

                play_name = "%s - %s" % (norm['composer']['name'], norm['work']['name'])
                # TODO: create separate hash sequence for top of each hour!!!
                play_seq = playlist.hash_seq.add(play_name)
                if play_seq:
                    ps_recs = MusicLib.insert_play_seq(play_rec, play_seq, 1)
                else:
                    log.debug("Skipping hash_seq for duplicate play:\n%s" % (play_rec))

        return pp_rec

    def map_program_play(self, data):
        """This is the implementation for WWFM (and others)

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
        tz = pytz.timezone(self.station.timezone)

        pp_data = {}
        pp_data['prog_play_info']  = data
        pp_data['prog_play_date']  = str2date(sdate)
        pp_data['prog_play_start'] = str2time(stime)
        pp_data['prog_play_end']   = str2time(etime)
        pp_data['prog_play_dur']   = None # Interval, if listed
        pp_data['notes']           = None # ARRAY(Text)
        pp_data['start_time']      = datetimetz(sdate, stime, tz)
        pp_data['end_time']        = datetimetz(edate, etime, tz)
        pp_data['duration']        = pp_data['end_time'] - pp_data['start_time']

        return {'program': prog_data, 'program_play': pp_data}

    def map_play(self, pp_rec, raw_data):
        """This is the implementation for WWFM (and others)

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
        sdate, stime = raw_data['_start_time'].split()
        if '_end_time' in raw_data:
            edate, etime = raw_data['_end_time'].split()
        else:
            edate, etime = (None, None)

        # NOTE: would like to do integrity check, but need to rectify formatting difference
        # for date, hour offset for time, non-empty value for _end!!!
        #if sdate != raw_data['_date']:
        #    log.debug("Date mismatch %s != %s" % (sdate, raw_data['date']))
        #if stime != raw_data['_start']:
        #    log.debug("Start time mismatch %s != %s" % (stime, raw_data['start_time']))
        #if etime != raw_data['_end']:
        #    log.debug("End time mismatch %s != %s" % (etime, raw_data['end_time']))

        dur_msecs = raw_data.get('_duration')
        tz = pytz.timezone(self.station.timezone)

        play_info = {}
        play_info['play_info']  = raw_data
        play_info['play_date']  = str2date(sdate, '%m-%d-%Y')
        play_info['play_start'] = str2time(stime)
        play_info['play_end']   = str2time(etime) if etime else None
        play_info['play_dur']   = dt.timedelta(0, 0, 0, dur_msecs) if dur_msecs else None
        play_info['notes']      = None # ARRAY(Text)
        play_info['start_time'] = datetimetz(play_info['play_date'], play_info['play_start'], tz)
        if etime:
            end_date = play_info['play_date'] if etime > stime else play_info['play_date'] + dt.timedelta(1)
            play_info['end_time'] = datetimetz(end_date, play_info['play_end'], tz)
            play_info['duration'] = play_info['end_time'] - play_info['start_time']
        else:
            play_info['end_time'] = None # TIMESTAMP(timezone=True)
            play_info['duration'] = None # Interval

        # do the easy stuff first (don't worry about empty records...for now!)
        composer_data   = {}
        work_data       = {}
        conductor_data  = {}
        performers_data = []
        ensembles_data  = []
        recording_data  = {'label'     : raw_data.get('copyright'),
                           'catalog_no': raw_data.get('catalogNumber'),
                           'name'      : raw_data.get('collectionName')}
        entity_str_data = {'composer'  : [],
                           'work'      : [],
                           'conductor' : [],
                           'performers': [],
                           'ensembles' : [],
                           'recording' : [recording_data['name']],
                           'label'     : [recording_data['label']]}

        # normalized return structure (build from above elements)
        play_data = ml_dict({'play':       play_info,
                             'composer':   composer_data,
                             'work':       work_data,
                             'conductor':  conductor_data,
                             'performers': performers_data,
                             'ensembles':  ensembles_data,
                             'recording':  recording_data,
                             'entity_str': entity_str_data})

        composer_str = raw_data.get('composerName')
        if composer_str:
            entity_str_data['composer'].append(composer_str)
            play_data.merge(parse_composer_str(composer_str))

        work_str = raw_data.get('trackName')
        if work_str:
            entity_str_data['work'].append(work_str)
            play_data.merge(parse_work_str(work_str))

        conductor_str = raw_data.get('conductor')
        if conductor_str:
            entity_str_data['conductor'].append(conductor_str)
            play_data.merge(parse_conductor_str(conductor_str))

        # FIX: artistName is used by different stations (and probably programs within
        # the same staion) to mean either ensembles or soloists; can probably mitigate
        # (somewhat) by mapping stations to different parsers, but ultimately need to
        # be able to parse all performer/ensemble information out of any field!!!
        for perf_str in (raw_data.get('artistName'), raw_data.get('soloists')):
            if perf_str:
                entity_str_data['performers'].append(perf_str)
                play_data.merge(parse_performer_str(perf_str))

        ensembles_str = raw_data.get('ensembles')
        if ensembles_str:
            entity_str_data['ensembles'].append(ensembles_str)
            play_data.merge(parse_ensemble_str(ensembles_str))

        return play_data

#+-----------+
#| ParserMPR |
#+-----------+

class ParserMPR(Parser):
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
            soup = BeautifulSoup(f, "lxml")

        title = soup.title.string.strip()
        m = re.match(r'Playlist for (\w+ \d+, \d+)', title)
        if not m:
            raise RuntimeError("Could not parse title \"%s\"" % (title))
        pl_date = dt.datetime.strptime(m.group(1), '%B %d, %Y').date()

        pl_root = soup.find('dl', id="playlist")
        for prog_head in reversed(pl_root('dt', recursive=False)):
            # Step 1 - Parse out program_play info
            norm = self.map_program_play(pl_date, prog_head)
            pp_rec = MusicLib.insert_program_play(playlist, norm)
            if not pp_rec:
                raise RuntimeError("Could not insert program_play")
            playlist.parse_ctx['station_id']   = pp_rec['station_id']
            playlist.parse_ctx['prog_play_id'] = pp_rec['id']
            playlist.parse_ctx['play_id']      = None
            pp_rec['plays'] = []

            # Step 2 - Parse out play info
            prog_body = prog_head.find_next_sibling('dd')
            for play_head in reversed(prog_body.ul('li', recursive=False)):
                norm = self.map_play(pp_rec, play_head)
                play_rec = MusicLib.insert_play(playlist, pp_rec, norm)
                if not play_rec:
                    raise RuntimeError("Could not insert play")
                playlist.parse_ctx['play_id'] = play_rec['id']
                pp_rec['plays'].append(play_rec)

                es_recs = MusicLib.insert_entity_strings(playlist, norm)

                play_name = "%s - %s" % (norm['composer']['name'], norm['work']['name'])
                # TODO: create separate hash sequence for top of each hour!!!
                play_seq = playlist.hash_seq.add(play_name)
                if play_seq:
                    ps_recs = MusicLib.insert_play_seq(play_rec, play_seq, 1)
                else:
                    log.debug("Skipping hash_seq for duplicate play:\n%s" % (play_rec))

        return pp_rec

    def map_program_play(self, pl_date, prog_head):
        """This is the implementation for MPR

        raw data in: bs4 'dt' tag
        normalized data out: {
            'program': {},
            'program_play': {}
        }
        """
        prog_name = prog_head.h2.string.strip().encode('utf-8')
        m = re.match(r'(\d+:\d+ (?:AM|PM)).+?(\d+:\d+ (?:AM|PM))', prog_name)
        start_time = dt.datetime.strptime(m.group(1), '%I:%M %p').time()
        end_time   = dt.datetime.strptime(m.group(2), '%I:%M %p').time()
        start_date = pl_date
        end_date   = pl_date if end_time > start_time else pl_date + dt.timedelta(1)
        tz         = pytz.timezone(self.station.timezone)
        # TODO: lookup host name from refdata!!!
        prog_data = {'name': prog_name}

        pp_data = {}
        # TODO: convert prog_head into dict for prog_play_info!!!
        pp_data['prog_play_info']  = {}
        pp_data['prog_play_date']  = start_date
        pp_data['prog_play_start'] = start_time
        pp_data['prog_play_end']   = end_time
        pp_data['prog_play_dur']   = None # Interval, if listed
        pp_data['notes']           = None # ARRAY(Text)
        pp_data['start_time']      = datetimetz(start_date, start_time, tz)
        pp_data['end_time']        = datetimetz(end_date, end_time, tz)
        pp_data['duration']        = pp_data['end_time'] - pp_data['start_time']

        return {'program': prog_data, 'program_play': pp_data}

    def map_play(self, pp_rec, play_head):
        """This is the implementation for MPR

        raw data in: bs4 'li' tag
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
        raw_data = {}
        pp_start = pp_rec['prog_play_start']
        play_start = play_head.find('a', class_="song-time").time
        start_date = play_start['datetime']
        start_time = play_start.string + ' ' + time2str(pp_start, '%p')
        raw_data['start_date'] = start_date  # %Y-%m-%d
        raw_data['start_time'] = start_time  # %I:%M %p (12-hour format)
        tz = pytz.timezone(self.station.timezone)

        buy_button = play_head.find('a', class_="buy-button", href=True)
        if (buy_button):
            res = urlsplit(buy_button['href'])
            url_fields = parse_qs(res.query, keep_blank_values=True)
            label = url_fields['label'][0]
            catalog_no = url_fields['catalog'][0]
            raw_data['label'] = label
            raw_data['catalog_no'] = catalog_no

        play_body = play_head.find('div', class_="song-info")
        for play_field in play_body(['h3', 'h4'], recusive=False):
            """
            song-title: Prelude
            song-composer: Walter Piston
            song-conductor: Carlos Kalmar
            song-orch_ensemble: Grant Park Orchestra
            song-soloist soloist-1: David Schrader, organ
            """
            field_name = play_field['class']
            if isinstance(field_name, list):
                field_name = ' '.join(field_name)
            field_value = play_field.string.strip()
            raw_data[field_name] = field_value or None

        play_info = {}
        # TODO: better conversion of play_head/play_body into dict for play_info!!!
        play_info['play_info']  = raw_data
        play_info['play_date']  = str2date(raw_data['start_date'])
        play_info['play_start'] = str2time(raw_data['start_time'], '%I:%M %p')
        play_info['play_end']   = None # Time
        play_info['play_dur']   = None # Interval
        play_info['notes']      = None # ARRAY(Text)
        play_info['start_time'] = datetimetz(play_info['play_date'], play_info['play_start'], tz)
        play_info['end_time']   = None # TIMESTAMP(timezone=True)
        play_info['duration']   = None # Interval

        composer_data  =  {}
        work_data      =  {}
        conductor_data =  {}
        performers_data = []
        ensembles_data  = []
        recording_data =  {'label'     : raw_data.get('label'),
                           'catalog_no': raw_data.get('catalog_no')}
        entity_str_data = {'composer'  : [],
                           'work'      : [],
                           'conductor' : [],
                           'performers': [],
                           'ensembles' : [],
                           'label'     : [recording_data['label']]}

        # normalized return structure (build from above elements)
        play_data = ml_dict({'play':       play_info,
                             'composer':   composer_data,
                             'work':       work_data,
                             'conductor':  conductor_data,
                             'performers': performers_data,
                             'ensembles':  ensembles_data,
                             'recording':  recording_data,
                             'entity_str': entity_str_data})

        composer_str = raw_data.get('song-composer')
        if composer_str:
            entity_str_data['composer'].append(composer_str)
            play_data.merge(parse_composer_str(composer_str))

        work_str = raw_data.get('song-title')
        if work_str:
            entity_str_data['work'].append(work_str)
            play_data.merge(parse_work_str(work_str))

        conductor_str = raw_data.get('song-conductor')
        if conductor_str:
            entity_str_data['conductor'].append(conductor_str)
            play_data.merge(parse_conductor_str(conductor_str))

        perf_str = raw_data.get('song-soloist soloist-1')
        if perf_str:
            entity_str_data['performers'].append(perf_str)
            play_data.merge(parse_performer_str(perf_str))

        ensembles_str = raw_data.get('song-orch_ensemble')
        if ensembles_str:
            entity_str_data['ensembles'].append(ensembles_str)
            play_data.merge(parse_ensemble_str(ensembles_str))

        return play_data

class ParserC24(Parser):
    def parse(self, playlist, dryrun = False, force = False):
        """Parse playlist, write to musiclib if not dryrun

        :param playlist: Playlist object
        :param dryrun: don't write to database
        :param force: overwrite program_play/play in databsae
        :return: dict with parsed program_play/play info
        """
        log.debug("Parsing json for %s", os.path.relpath(playlist.file, playlist.station.station_dir))
        with open(playlist.file) as f:
            soup = BeautifulSoup(f, "lxml")

        top = soup.find('a', attrs={'name': 'top'})
        tab = top.find_next('table')
        pl_head = tab('tr', recursive=False)[0]
        pl_body = tab('tr', recursive=False)[1]

        title = pl_head.find('span', class_='title')
        datestr = title.find_next_sibling('i').string  # "Monday, September 17, 2018 Central Time"
        m = re.match(r'(\w+), (\w+ {1,2}\d+, \d+) (.+)', datestr)
        if not m:
            raise RuntimeError("Could not parse datestr \"%s\"" % (datestr))
        pl_date = dt.datetime.strptime(m.group(2), '%B %d, %Y').date()

        prog_divs = [rule.parent for rule in pl_body.select('div > hr')]
        for prog_div in prog_divs:
            # Step 1 - Parse out program_play info
            prog_head = prog_div.find_next('p')
            if not prog_head:
                break
            norm = self.map_program_play(pl_date, prog_head)
            pp_rec = MusicLib.insert_program_play(playlist, norm)
            if not pp_rec:
                raise RuntimeError("Could not insert program_play")
            playlist.parse_ctx['station_id']   = pp_rec['station_id']
            playlist.parse_ctx['prog_play_id'] = pp_rec['id']
            playlist.parse_ctx['play_id']      = None
            pp_rec['plays'] = []

            # Step 2 - Parse out play info
            play_heads = prog_div.find_next_siblings(['table', 'div'])
            for play_head in play_heads:
                if play_head.name == 'div':
                    break
                norm = self.map_play(pp_rec, play_head)
                play_rec = MusicLib.insert_play(playlist, pp_rec, norm)
                if not play_rec:
                    raise RuntimeError("Could not insert play")
                playlist.parse_ctx['play_id'] = play_rec['id']
                pp_rec['plays'].append(play_rec)

                es_recs = MusicLib.insert_entity_strings(playlist, norm)

                play_name = "%s - %s" % (norm['composer']['name'], norm['work']['name'])
                # TODO: create separate hash sequence for top of each hour!!!
                play_seq = playlist.hash_seq.add(play_name)
                if play_seq:
                    ps_recs = MusicLib.insert_play_seq(play_rec, play_seq, 1)
                else:
                    log.debug("Skipping hash_seq for duplicate play:\n%s" % (play_rec))

        return pp_rec

    def map_program_play(self, pl_date, prog_head):
        """This is the implementation for C24

        raw data in: bs4 'p' tag
        normalized data out: {
            'program': {},
            'program_play': {}
        }
        """
        prog_name = prog_head.string.strip()  # "MID -  1AM"
        prog_times = prog_name.replace('MID', '12AM').replace('12N', '12PM')
        m = re.match(r'(\d+(?:AM|PM)).+?(\d+(?:AM|PM))', prog_times)
        if not m:
            raise RuntimeError("Could not parse prog_times \"%s\"" % (prog_times))
        start_time = dt.datetime.strptime(m.group(1), '%I%p').time()
        end_time   = dt.datetime.strptime(m.group(2), '%I%p').time()
        start_date = pl_date
        end_date   = pl_date if end_time > start_time else pl_date + dt.timedelta(1)
        tz         = pytz.timezone(self.station.timezone)
        # TODO: lookup host name from refdata!!!
        prog_data = {'name': prog_name}

        pp_data = {}
        # TODO: convert prog_head into dict for prog_play_info!!!
        pp_data['prog_play_info']  = {}
        pp_data['prog_play_date']  = start_date
        pp_data['prog_play_start'] = start_time
        pp_data['prog_play_end']   = end_time
        pp_data['prog_play_dur']   = None # Interval, if listed
        pp_data['notes']           = None # ARRAY(Text)
        pp_data['start_time']      = datetimetz(start_date, start_time, tz)
        pp_data['end_time']        = datetimetz(end_date, end_time, tz)
        pp_data['duration']        = pp_data['end_time'] - pp_data['start_time']

        return {'program': prog_data, 'program_play': pp_data}

    def map_play(self, pp_rec, play_head):
        """This is the implementation for C24

        raw data in: bs4 'table' tag
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
        raw_data = {}
        processed = set()
        elems = play_head.tr('td', recursive=False)
        play_start = elems[0]
        play_body = elems[1]

        # REVISIT: kind of stupid, but we do this for consistency with MPR
        # (to make abstraction easier at some point); all of this really
        # needs to be cleaned up!!!
        pp_date = pp_rec['prog_play_date']
        start_date = date2str(pp_date)
        start_time = play_start.string.strip()  # "12:01AM"
        raw_data['start_date'] = start_date  # %Y-%m-%d
        raw_data['start_time'] = start_time  # %I:%M%p (12-hour format)
        tz = pytz.timezone(self.station.timezone)

        # Step 2a - try and find label information (<i>...</i> - <a href=...>)
        rec_center = play_body.find(string=re.compile(r'\s+\-\s+$'))
        rec_listing = rec_center.previous_sibling
        #m = re.fullmatch(r'(.*\S) (\w+)', rec_listing.string)
        m = re.match(r'(.*\S) (\w+)$', rec_listing.string)
        if m:
            raw_data['label'] = m.group(1)
            raw_data['catalog_no'] = m.group(2)
            processed.add(rec_listing.string)
        rec_buy_url = rec_center.next_sibling
        # Step 2b - get as much info as we can from the "BUY" url
        if rec_buy_url.name == 'a':
            res = urlsplit(rec_buy_url['href'])
            url_fields = parse_qs(res.query, keep_blank_values=True)
            label      = url_fields['label'][0]
            catalog_no = url_fields['catalog'][0]
            composer   = url_fields['composer'][0]
            work       = url_fields['work'][0]
            url_title  = "%s - %s" % (composer, work)
            url_rec    = "%s %s" % (label, catalog_no)
            raw_data['composer'] = composer
            raw_data['work'] = work
            processed.add(url_title)
            if raw_data.get('label') and raw_data.get('catalog_no'):
                if label != raw_data['label'] or catalog_no != raw_data['catalog_no']:
                    log.debug("Recording in URL (%s) mismatch with listing (%s)" %
                              (url_rec, rec_listing.string))
                elif url_rec != rec_listing.string:
                    raise RuntimeError("Rec string mismatch \"%s\" != \"%s\"",
                                       (url_rec, rec_listing.string))
            else:
                if raw_data.get('label') or raw_data.get('catalog_no'):
                    log.debug("Overwriting listing (%s) with recording from URL (%s)" %
                              (rec_listing.string, url_rec))
                raw_data['label'] = label
                raw_data['catalog_no'] = catalog_no
                processed.add(url_rec)
        else:
            raise RuntimeError("Expected <a>, got <%s> instead" % (rec_buy_url.name))

        # Step 2c - now parse the individual text fields, skipping and/or validating
        #           stuff we've already parsed out (absent meta-metadata)
        for field in play_body.find_all(['b', 'i']):
            if field.string in processed:
                #log.debug("Skipping field \"%s\", already parsed" % (field.string))
                continue
            #m = re.fullmatch(r'(.+), ([\w\./ ]+)', field.string)
            m = re.match(r'(.+), ([\w\.\'/ -]+)$', field.string)
            if m:
                if m.group(2).lower() in COND_STRS:
                    raw_data['conductor'] = m.group(1)
                else:
                    raw_data['performer'] = field.string
            else:
                subfields = field.string.split(' - ')
                if len(subfields) == 2 and subfields[0][-1] != ' ' and subfields[1][0] != ' ':
                    composer = subfields[0]
                    work = subfields[1]
                    if raw_data.get('composer'):
                        log.debug("Overwriting composer \"%s\" with \"%s\"" %
                                  (raw_data['composer'], composer))
                        raw_data['composer'] = composer
                    if raw_data.get('work'):
                        # HACK: catch unicode problem--FIX with migration to python3!!!
                        try:
                            log.debug("Overwriting work \"%s\" with \"%s\"" %
                                      (raw_data['work'], work))
                        except UnicodeDecodeError:
                            log.debug("Overwriting work \"%s\" with \"%s\"" %
                                      ("<blah blah blah>", "<blah blah blah>"))
                        raw_data['work'] = work
                else:
                    # REVISIT: for now, just assume we have an ensemble name, though we can't
                    # really know what to do on conflict unless/until we parse the contents and
                    # categorize properly (though, could also add both and debug later)!!!
                    if raw_data.get('ensemble'):
                        if field.string.lower() in SKIP_ENS:
                            log.debug("Don't overwrite ensemble \"%s\" with \"%s\" (SKIP_ENS)" %
                                      (raw_data['ensemble'], field.string))
                            continue
                        raise RuntimeError("Can't overwrite ensemble \"%s\" with \"%s\"" %
                                           (raw_data['ensemble'], field.string))
                    raw_data['ensemble'] = field.string

        play_info = {}
        # TODO: better conversion of play_head/play_body into dict for play_info!!!
        play_info['play_info']  = raw_data
        play_info['play_date']  = str2date(raw_data['start_date'])
        play_info['play_start'] = str2time(raw_data['start_time'], '%I:%M%p')
        play_info['play_end']   = None # Time
        play_info['play_dur']   = None # Interval
        play_info['notes']      = None # ARRAY(Text)
        play_info['start_time'] = datetimetz(play_info['play_date'], play_info['play_start'], tz)
        play_info['end_time']   = None # TIMESTAMP(timezone=True)
        play_info['duration']   = None # Interval

        composer_data   = {}
        work_data       = {}
        conductor_data  = {}
        performers_data = []
        ensembles_data  = []
        recording_data  = {'label'     : raw_data.get('label'),
                           'catalog_no': raw_data.get('catalog_no')}
        entity_str_data = {'composer'  : [],
                           'work'      : [],
                           'conductor' : [],
                           'performers': [],
                           'ensembles' : [],
                           'label'     : [recording_data['label']]}

        # normalized return structure (build from above elements)
        play_data = ml_dict({'play':       play_info,
                             'composer':   composer_data,
                             'work':       work_data,
                             'conductor':  conductor_data,
                             'performers': performers_data,
                             'ensembles':  ensembles_data,
                             'recording':  recording_data,
                             'entity_str': entity_str_data})

        composer_str = raw_data.get('composer')
        if composer_str:
            entity_str_data['composer'].append(composer_str)
            play_data.merge(parse_composer_str(composer_str))

        work_str = raw_data.get('work')
        if work_str:
            entity_str_data['work'].append(work_str)
            play_data.merge(parse_work_str(work_str))

        conductor_str = raw_data.get('conductor')
        if conductor_str:
            entity_str_data['conductor'].append(conductor_str)
            play_data.merge(parse_conductor_str(conductor_str))

        perf_str = raw_data.get('performer')
        if perf_str:
            entity_str_data['performers'].append(perf_str)
            play_data.merge(parse_performer_str(perf_str))

        ensembles_str = raw_data.get('ensemble')
        if ensembles_str:
            entity_str_data['ensembles'].append(ensembles_str)
            play_data.merge(parse_ensemble_str(ensembles_str))

        return play_data

#####################
# command line tool #
#####################

import click
import station

@click.command()
@click.option('--list',      'cmd', flag_value='list', default=True, help="List all (or specified) playlists")
@click.option('--parse',     'cmd', flag_value='parse', help="Parse out play information from playlist")
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

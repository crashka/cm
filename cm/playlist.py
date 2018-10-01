#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Playlist module
"""

from __future__ import absolute_import, division, print_function

import os.path
import logging
import json
import re
import datetime as dt
from urlparse import urlsplit, parse_qs

from bs4 import BeautifulSoup
import click

import core
import station
from musiclib import MusicLib, COND_STRS
from datasci import HashSeq
from utils import LOV, prettyprint, str2date, date2str, str2time, time2str, strtype, collecttype

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

    Note: probably could (should???) implement all methods as static
    """
    def __init__(self):
        """Parser object is stateless, so constructor doesn't do anything
        """
        pass

    def parse(self, playlist, dryrun = False, force = False):
        """Abstract method for parse operation
        """
        raise RuntimeError("parse() not implemented in subclass (or base class called)")

##########################
# Parser adapter classes #
##########################

def get_parser(cls_name):
    cls = globals().get(cls_name)
    return cls() if cls else None

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
            # TEMP: get rid of dev/debugging stuff (careful, prog_name used below)!!!
            prog_info = prog.get('program')
            assert isinstance(prog_info, dict)
            prog_desc = prog_info.get('program_desc')
            prog_name = prog_info.get('name') + (' - ' + prog_desc if prog_desc else '')
            log.debug("PROGRAM [%s]: %s" % (prog['fullstart'], prog_name))
            prog_copy = prog.copy()
            del prog_copy['playlist']

            # Step 1 - Parse out program_play info
            norm = self.map_program_play(prog)
            pp_rec = MusicLib.insert_program_play(playlist, norm)
            if not pp_rec:
                raise RuntimeError("Could not insert program_play")
            else:
                log.debug("Created program_play ID %d" % (pp_rec['id']))
            pp_rec['plays'] = []

            # Step 2 - Parse out play info (if present)
            plays = prog.get('playlist') or []
            for play in plays:
                norm = self.map_play(play)
                play_rec = MusicLib.insert_play(playlist, pp_rec, norm)
                if not play_rec:
                    raise RuntimeError("Could not insert play")
                else:
                    log.debug("Created play ID %d" % (play_rec['id']))
                pp_rec['plays'].append(play_rec)

                play_name = "%s - %s" % (play.get('composerName'), play.get('trackName'))
                # TODO: create separate hash sequence for top of each hour!!!
                play_seq = playlist.hash_seq.add(play_name)
                if play_seq:
                    ps_recs = MusicLib.insert_play_seq(play_rec, play_seq, 1)
                else:
                    log.debug("Skipping hash_seq for duplicate play:\n%s" % (play_rec))

        return pp_rec

    def map_program_play(self, data):
        """This is the implementation for WWFM

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
        """This is the implementation for WWFM

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
            mkperf = lambda name, role: {'person': {'name': name}, 'role': role}
            if not perf_str:
                continue
            if ';' in perf_str:
                perfs = perf_str.split(';')
                for perf in perfs:
                    fields = perf.rsplit(',', 1)
                    if len(fields) == 1:
                        performers_data.append(mkperf(fields[0], None))
                    else:
                        performers_data.append(mkperf(fields[0].strip(), fields[1].strip()))
            elif perf_str.count(',') % 2 == 1:
                fields = perf_str.split(',')
                while fields:
                    pers, role = (fields.pop(0), fields.pop(0))
                    performers_data.append(mkperf(pers.strip(), role.strip()))
            else:
                # TODO: if even number of commas, need to look closer at string contents/format
                # to figure out what to do!!!
                performers_data.append(mkperf(perf_str, None))

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
            else:
                log.debug("Created program_play ID %d" % (pp_rec['id']))
            pp_start = pp_rec['prog_play_start']
            pp_rec['plays'] = []

            # Step 2 - Parse out play info
            prog_body = prog_head.find_next_sibling('dd')
            for play_head in reversed(prog_body.ul('li', recursive=False)):
                norm = self.map_play(pp_start, play_head)
                play_rec = MusicLib.insert_play(playlist, pp_rec, norm)
                if not play_rec:
                    raise RuntimeError("Could not insert play")
                else:
                    log.debug("Created play ID %d" % (play_rec['id']))
                pp_rec['plays'].append(play_rec)

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
        end_time = dt.datetime.strptime(m.group(2), '%I:%M %p').time()
        # TODO: lookup host name from refdata!!!
        prog_data = {'name': prog_name}

        pp_data = {}
        # TODO: convert prog_head into dict for prog_play_info!!!
        pp_data['prog_play_info'] =  {}
        pp_data['prog_play_date'] =  pl_date
        pp_data['prog_play_start'] = start_time
        pp_data['prog_play_end'] =   end_time
        pp_data['prog_play_dur'] =   None # Interval, if listed
        pp_data['notes'] =           None # ARRAY(Text)),
        pp_data['start_time'] =      None # TIMESTAMP(timezone=True)),
        pp_data['end_time'] =        None # TIMESTAMP(timezone=True)),
        pp_data['duration'] =        None # Interval)

        return {'program': prog_data, 'program_play': pp_data}

    def map_play(self, pp_start, play_head):
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
        data = {}
        play_start = play_head.find('a', class_="song-time").time
        start_date = play_start['datetime']
        start_time = play_start.string + ' ' + time2str(pp_start, '%p')
        data['start_date'] = start_date  # %Y-%m-%d
        data['start_time'] = start_time  # %H:%M %p (12-hour format)

        buy_button = play_head.find('a', class_="buy-button", href=True)
        if (buy_button):
            res = urlsplit(buy_button['href'])
            url_fields = parse_qs(res.query, keep_blank_values=True)
            label = url_fields['label'][0]
            catalog_no = url_fields['catalog'][0]
            data['label'] = label
            data['catalog_no'] = catalog_no

        play_body = play_head.find('div', class_="song-info")
        for play_field in play_body(['h3', 'h4'], recusive=False):
            field_name = play_field['class']
            if isinstance(field_name, list):
                field_name = ' '.join(field_name)
            field_value = play_field.string.strip()
            data[field_name] = field_value or None
            """
            song-title: Prelude
            song-composer: Walter Piston
            song-conductor: Carlos Kalmar
            song-orch_ensemble: Grant Park Orchestra
            song-soloist soloist-1: David Schrader, organ
            """
        composer_data  =  {'name'      : data.get('song-composer')}
        work_data      =  {'name'      : data.get('song-title')}
        conductor_data =  {'name'      : data.get('song-conductor')}
        recording_data =  {'label'     : data.get('label'),
                           'catalog_no': data.get('catalog_no')}

        # FIX/NO MORE COPY-PASTE: need to abstract out the logic for performers and ensembles
        # across parser subclasses!!!
        performers_data = []
        perf_str = data.get('song-soloist soloist-1')
        if perf_str:
            mkperf = lambda name, role: {'person': {'name': name}, 'role': role}
            if ';' in perf_str:
                perfs = perf_str.split(';')
                for perf in perfs:
                    fields = perf.rsplit(',', 1)
                    if len(fields) == 1:
                        performers_data.append(mkperf(fields[0], None))
                    else:
                        performers_data.append(mkperf(fields[0].strip(), fields[1].strip()))
            elif perf_str.count(',') % 2 == 1:
                fields = perf_str.split(',')
                while fields:
                    pers, role = (fields.pop(0), fields.pop(0))
                    performers_data.append(mkperf(pers.strip(), role.strip()))
            else:
                # TODO: if even number of commas, need to look closer at string contents/format
                # to figure out what to do!!!
                performers_data.append(mkperf(perf_str, None))

        # FIX: see above!!!
        ensembles_data  =  []
        ensembles_str = data.get('song-orch_ensemble')
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

        play_data = {}
        # TODO: convert play_body into dict for play_info!!!
        play_data['play_info'] =  {}
        play_data['play_date'] =  str2date(data['start_date'])
        play_data['play_start'] = str2time(data['start_time'], '%H:%M %p')
        play_data['play_end'] =   None # Time
        play_data['play_dur'] =   None # Interval
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
        m = re.match(r'(\w+), (\w+ \d+, \d+) (.+)', datestr)
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
            else:
                log.debug("Created program_play ID %d" % (pp_rec['id']))
            pp_date = pp_rec['prog_play_date']
            pp_rec['plays'] = []

            # Step 2 - Parse out play info
            play_heads = prog_div.find_next_siblings(['table', 'div'])
            for play_head in play_heads:
                if play_head.name == 'div':
                    break
                norm = self.map_play(pp_date, play_head)
                play_rec = MusicLib.insert_play(playlist, pp_rec, norm)
                if not play_rec:
                    raise RuntimeError("Could not insert play")
                else:
                    log.debug("Created play ID %d" % (play_rec['id']))
                pp_rec['plays'].append(play_rec)

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
        end_time = dt.datetime.strptime(m.group(2), '%I%p').time()
        # TODO: lookup host name from refdata!!!
        prog_data = {'name': prog_name}

        pp_data = {}
        # TODO: convert prog_head into dict for prog_play_info!!!
        pp_data['prog_play_info'] =  {}
        pp_data['prog_play_date'] =  pl_date
        pp_data['prog_play_start'] = start_time
        pp_data['prog_play_end'] =   end_time
        pp_data['prog_play_dur'] =   None # Interval, if listed
        pp_data['notes'] =           None # ARRAY(Text)),
        pp_data['start_time'] =      None # TIMESTAMP(timezone=True)),
        pp_data['end_time'] =        None # TIMESTAMP(timezone=True)),
        pp_data['duration'] =        None # Interval)

        return {'program': prog_data, 'program_play': pp_data}

    def map_play(self, pp_date, play_head):
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
        data = {}
        processed = set()
        elems = play_head.tr('td', recursive=False)
        play_start = elems[0]
        play_body = elems[1]

        # REVISIT: kind of stupid, but we do this for consistency with MPR
        # (to make abstraction easier at some point); all of this really
        # needs to be cleaned up!!!
        start_date = date2str(pp_date)
        start_time = play_start.string.strip()  # "12:01AM"
        data['start_date'] = start_date  # %Y-%m-%d
        data['start_time'] = start_time  # %H:%M%p (12-hour format)

        # Step 2a - try and find label information (<i>...</i> - <a href=...>)
        rec_center = play_body.find(string=re.compile(r'\s+\-\s+$'))
        rec_listing = rec_center.previous_sibling
        m = re.match(r'(.*\S) (\w+)$', rec_listing.string)
        if m:
            data['label'] = m.group(1)
            data['catalog_no'] = m.group(2)
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
            data['composer'] = composer
            data['work'] = work
            processed.add(url_title)
            if data.get('label') and data.get('catalog_no'):
                if label != data['label'] or catalog_no != data['catalog_no']:
                    log.debug("Recording in URL (%s) mismatch with listing (%s)" %
                              (url_rec, rec_listing.string))
                elif url_rec != rec_listing.string:
                    raise RuntimeError("Rec string mismatch \"%s\" != \"%s\"",
                                       (url_rec, rec_listing.string))
            else:
                if data.get('label') or data.get('catalog_no'):
                    log.debug("Overwriting listing (%s) with recording from URL (%s)" %
                              (rec_listing.string, url_rec))
                data['label'] = label
                data['catalog_no'] = catalog_no
                processed.add(url_rec)
        else:
            raise RuntimeError("Expected <a>, got <%s> instead" % (rec_buy_url.name))

        # Step 2c - now parse the individual text fields, skipping and/or validating
        #           stuff we've already parsed out (absent meta-metadata)
        for field in play_body.find_all(['b', 'i']):
            if field.string in processed:
                #log.debug("Skipping field \"%s\", already parsed" % (field.string))
                continue
            m = re.match(r'(.+), ([\w\./ ]+)$', field.string)
            if m:
                if m.group(2).lower() in COND_STRS:
                    data['conductor'] = m.group(1)
                else:
                    data['performer'] = field.string
            else:
                subfields = field.string.split(' - ')
                if len(subfields) == 2 and subfields[0][-1] != ' ' and subfields[1][0] != ' ':
                    composer = subfields[0]
                    work = subfields[1]
                    if data.get('composer'):
                        log.debug("Overwriting composer \"%s\" with \"%s\"" %
                                  (data['composer'], composer))
                        data['composer'] = composer
                    if data.get('work'):
                        log.debug("Overwriting work \"%s\" with \"%s\"" %
                                  (data['work'], work))
                        data['work'] = work
                else:
                    # REVISIT: for now, just assume we have an ensemble name, though we can't
                    # really know what to do on conflict unless/until we parse the contents and
                    # categorize properly (though, could also add both and debug later)!!!
                    if data.get('ensemble'):
                        raise RuntimeError("Can't overwrite ensemble \"%s\" with \"%s\"" %
                                           (data['ensemble'], field.string))
                    data['ensemble'] = field.string

        composer_data  =  {'name'      : data.get('composer')}
        work_data      =  {'name'      : data.get('work')}
        conductor_data =  {'name'      : data.get('conductor')}
        recording_data =  {'label'     : data.get('label'),
                           'catalog_no': data.get('catalog_no')}

        # FIX/NO MORE COPY-PASTE: need to abstract out the logic for performers and ensembles
        # across parser subclasses!!!
        performers_data = []
        perf_str = data.get('performer')
        if perf_str:
            mkperf = lambda name, role: {'person': {'name': name}, 'role': role}
            if ';' in perf_str:
                perfs = perf_str.split(';')
                for perf in perfs:
                    fields = perf.rsplit(',', 1)
                    if len(fields) == 1:
                        performers_data.append(mkperf(fields[0], None))
                    else:
                        performers_data.append(mkperf(fields[0].strip(), fields[1].strip()))
            elif perf_str.count(',') % 2 == 1:
                fields = perf_str.split(',')
                while fields:
                    pers, role = (fields.pop(0), fields.pop(0))
                    performers_data.append(mkperf(pers.strip(), role.strip()))
            else:
                # TODO: if even number of commas, need to look closer at string contents/format
                # to figure out what to do!!!
                performers_data.append(mkperf(perf_str, None))

        # FIX: see above!!!
        ensembles_data  =  []
        ensembles_str = data.get('ensemble')
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

        play_data = {}
        # TODO: convert play_body into dict for play_info!!!
        play_data['play_info'] =  {}
        play_data['play_date'] =  str2date(data['start_date'])
        play_data['play_start'] = str2time(data['start_time'], '%H:%M%p')
        play_data['play_end'] =   None # Time
        play_data['play_dur'] =   None # Interval
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

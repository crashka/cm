#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Playlist module
"""

import os.path
import logging
import json
import regex as re
import datetime as dt
from urllib.parse import urlsplit, parse_qs

import pytz
from bs4 import BeautifulSoup

from core import cfg, env, log, dbg_hand, DFLT_HTML_PARSER

from musiclib import MusicLib, SKIP_ENS, ml_dict
from datasci import HashSeq
from utils import (LOV, prettyprint, str2date, date2str, str2time, time2str, datetimetz,
                   strtype, collecttype)

##############################
# common constants/functions #
##############################

INFO_KEYS    = {'sta_name',
                'datestr',
                'name',
                'status',
                'file',
                'parsed_info'}
NOPRINT_KEYS = {'parsed_info'}

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
    """Helper class for Playlist, proper subclass is associated through station config
    """
    @staticmethod
    def get(sta):
        """
        :param sta: Station (to be bound to parser)
        :return: instantiated Parser subclass
        """
        cls = globals().get(sta.parser_cls)
        return cls(sta) if cls else None

    def __init__(self, sta):
        """Parser object is relatively stateless (just station backreference and parser
        directives), so constructor doesn't really do anything
        """
        self.station = sta
        self.html_parser = env.get('html_parser') or DFLT_HTML_PARSER
        log.debug("HTML parser: %s" % (self.html_parser))
        self.ml = MusicLib()

    def iter_program_plays(self, playlist):
        """Iterator for program plays within a playlist, yield value is passed into
        map_program_play() and iter_plays())

        :param playlist: Playlist object
        :yield: subclass-specific program play representation
        """
        raise RuntimeError("abstract method iter_program_plays() must be subclassed")

    def iter_plays(self, prog_play):
        """Iterator for plays within a program play, yield value is passed into map_play()

        :param prog_play: yield value from iter_program_plays()
        :yield: subclass-specific play representation
        """
        raise RuntimeError("abstract method iter_plays() must be subclassed")

    def map_program_play(self, prog_play):
        """
        :param prog_play: yield value from iter_program_plays()
        :return: dict of normalized program play information
        """
        raise RuntimeError("abstract method map_program_play() must be subclassed")

    def map_play(self, pp_data, play):
        """
        :param pp_data: [dict] parent program play data (from map_program_play())
        :param play: yield value from iter_plays()
        :return: dict of normalized play information
        """
        raise RuntimeError("abstract method map_play() must be subclassed")

    def parse(self, playlist, dryrun = False, force = False):
        """Parse playlist, write to musiclib if not dryrun

        :param playlist: Playlist object
        :param dryrun: don't write to database
        :param force: overwrite program_play/play in databsae
        :return: dict with parsed program_play/play info
        """
        for prog in self.iter_program_plays(playlist):
            # Step 1 - Parse out program_play info
            pp_norm = self.map_program_play(prog)
            pp_rec = self.ml.insert_program_play(playlist, pp_norm)
            if not pp_rec:
                raise RuntimeError("Could not insert program_play")
            playlist.parse_ctx['station_id']   = pp_rec['station_id']
            playlist.parse_ctx['prog_play_id'] = pp_rec['id']
            playlist.parse_ctx['play_id']      = None
            pp_rec['plays'] = []

            # Step 2 - Parse out play info (if present)
            for play in self.iter_plays(prog):
                play_norm, entity_str_data = self.map_play(pp_norm['program_play'], play)
                # APOLOGY: perhaps this parsing of entity strings and merging into normalized
                # play data really belongs in the subclasses, but just hate to see all of the
                # exact replication of code--thus, we have this ugly, ill-defined interface,
                # oh well... (just need to be careful here)
                for composer_str in entity_str_data['composer']:
                    if composer_str:
                        play_norm.merge(self.ml.parse_composer_str(composer_str))
                for work_str in entity_str_data['work']:
                    if work_str:
                        play_norm.merge(self.ml.parse_work_str(work_str))
                for conductor_str in entity_str_data['conductor']:
                    if conductor_str:
                        play_norm.merge(self.ml.parse_conductor_str(conductor_str))
                for performers_str in entity_str_data['performers']:
                    if performers_str:
                        play_norm.merge(self.ml.parse_performer_str(performers_str))
                for ensembles_str in entity_str_data['ensembles']:
                    if ensembles_str:
                        play_norm.merge(self.ml.parse_ensemble_str(ensembles_str))

                play_rec = self.ml.insert_play(playlist, pp_rec, play_norm)
                if not play_rec:
                    raise RuntimeError("Could not insert play")
                playlist.parse_ctx['play_id'] = play_rec['id']
                pp_rec['plays'].append(play_rec)

                es_recs = self.ml.insert_entity_strings(playlist, entity_str_data)

                play_name = "%s - %s" % (play_norm['composer']['name'], play_norm['work']['name'])
                # TODO: create separate hash sequence for top of each hour!!!
                play_seq = playlist.hash_seq.add(play_name)
                if play_seq:
                    ps_recs = self.ml.insert_play_seq(play_rec, play_seq, 1)
                else:
                    log.debug("Skipping hash_seq for duplicate play:\n%s" % (play_rec))

        return pp_rec

##########################
# Parser adapter classes #
##########################

# TODO: move to subdirectory(ies) when this gets too unwieldy!!!

#+------------+
#| ParserWWFM |
#+------------+

class ParserWWFM(Parser):
    """Parser for WWFM-family of stations
    """
    def iter_program_plays(self, playlist):
        """This is the implementation for WWFM (and others)

        :param playlist: Playlist object
        :yield: [list of dicts] 'onToday' item from WWFM playlist file
        """
        log.debug("Parsing json for %s", os.path.relpath(playlist.file, playlist.station.station_dir))
        with open(playlist.file) as f:
            pl_info = json.load(f)
        pl_params = pl_info['params']
        pl_progs = pl_info['onToday']
        for prog in pl_progs:
            yield prog
        return

    def iter_plays(self, prog):
        """This is the implementation for WWFM (and others)

        :param prog: [list of dicts] yield value from iter_program_plays()
        :yield: 'playlist' item from WWFM playlist file
        """
        plays = prog.get('playlist') or []
        for play in plays:
            yield play
        return

    def map_program_play(self, data):
        """This is the implementation for WWFM (and others)

        raw data in: [list of dicts] 'onToday' item from WWFM playlist file
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
        # TODO: appropriate fixup of data (e.g. NULL chars) for prog_play_info!!!
        #pp_data['prog_play_info']  = data
        pp_data['prog_play_info']  = {}
        pp_data['prog_play_date']  = str2date(sdate)
        pp_data['prog_play_start'] = str2time(stime)
        pp_data['prog_play_end']   = str2time(etime)
        pp_data['prog_play_dur']   = None # Interval, if listed
        pp_data['notes']           = None # ARRAY(Text)
        pp_data['start_time']      = datetimetz(sdate, stime, tz)
        pp_data['end_time']        = datetimetz(edate, etime, tz)
        pp_data['duration']        = pp_data['end_time'] - pp_data['start_time']

        return {'program': prog_data, 'program_play': pp_data}

    def map_play(self, pp_data, raw_data):
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

        # special fix-up for NULL characters in recording name (WXXI)
        rec_name = raw_data.get('collectionName')
        if rec_name and '\u0000' in rec_name:
            raw_data['collectionName'] = rec_name.replace('\u0000', '') or None
            # TEMP: also remove itunes link from 'buy' element, since it also contains NULL
            if 'buy' in raw_data and 'itunes' in raw_data['buy']:
                del raw_data['buy']['itunes']

        play_data = {}
        play_data['play_info']  = raw_data
        play_data['play_date']  = str2date(sdate, '%m-%d-%Y')
        play_data['play_start'] = str2time(stime)
        play_data['play_end']   = str2time(etime) if etime else None
        play_data['play_dur']   = dt.timedelta(0, 0, 0, dur_msecs) if dur_msecs else None
        play_data['notes']      = None # ARRAY(Text)
        play_data['start_time'] = datetimetz(play_data['play_date'], play_data['play_start'], tz)
        if etime:
            end_date = play_data['play_date'] if etime > stime else play_data['play_date'] + dt.timedelta(1)
            play_data['end_time'] = datetimetz(end_date, play_data['play_end'], tz)
            play_data['duration'] = play_data['end_time'] - play_data['start_time']
        else:
            play_data['end_time'] = None # TIMESTAMP(timezone=True)
            play_data['duration'] = None # Interval

        rec_data  = {'name'      : raw_data.get('collectionName'),
                     'label'     : raw_data.get('copyright'),
                     'catalog_no': raw_data.get('catalogNumber')}

        # FIX: artistName is used by different stations (and probably programs within
        # the same staion) to mean either ensembles or soloists; can probably mitigate
        # (somewhat) by mapping stations to different parsers, but ultimately need to
        # be able to parse all performer/ensemble information out of any field!!!
        entity_str_data = {'composer'  : [raw_data.get('composerName')],
                           'work'      : [raw_data.get('trackName')],
                           'conductor' : [raw_data.get('conductor')],
                           'performers': [raw_data.get('artistName'),
                                          raw_data.get('soloists')],
                           'ensembles' : [raw_data.get('ensembles')],
                           'recording' : [rec_data['name']],
                           'label'     : [rec_data['label']]}

        return (ml_dict({'play':       play_data,
                         'composer':   {},
                         'work':       {},
                         'conductor':  {},
                         'performers': [],
                         'ensembles':  [],
                         'recording':  rec_data}),
                entity_str_data)

#+-----------+
#| ParserMPR |
#+-----------+

class ParserMPR(Parser):
    """Parser for MPR station
    """
    def iter_program_plays(self, playlist):
        """This is the implementation for MPR

        :param playlist: Playlist object
        :yield: [tuple] (pl_date, bs4 'dt' tag)
        """
        log.debug("Parsing html for %s", os.path.relpath(playlist.file, playlist.station.station_dir))
        with open(playlist.file) as f:
            soup = BeautifulSoup(f, self.html_parser)

        title = soup.title.string.strip()
        m = re.match(r'Playlist for (\w+ \d+, \d+)', title)
        if not m:
            raise RuntimeError("Could not parse title \"%s\"" % (title))
        pl_date = dt.datetime.strptime(m.group(1), '%B %d, %Y').date()

        pl_root = soup.find('dl', id="playlist")
        for prog_head in reversed(pl_root('dt', recursive=False)):
            yield (pl_date, prog_head)
        return

    def iter_plays(self, prog):
        """This is the implementation for MPR

        :param prog: [tuple] yield value from iter_program_plays()
        :yield: bs4 'li' tag
        """
        pl_date, prog_head = prog
        prog_body = prog_head.find_next_sibling('dd')
        for play_head in reversed(prog_body.ul('li', recursive=False)):
            yield play_head
        return

    def map_program_play(self, prog_info):
        """This is the implementation for MPR

        raw data in: [tuple] (pl_date, bs4 'dt' tag)
        normalized data out: {
            'program': {},
            'program_play': {}
        }
        """
        pl_date, prog_head = prog_info
        prog_name = prog_head.h2.string.strip()
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

    def map_play(self, pp_data, play_head):
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
        pp_start = pp_data['prog_play_start']
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
            label = url_fields['label'][0] if url_fields.get('label') else None
            catalog_no = url_fields['catalog'][0] if url_fields.get('catalog') else None
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

        play_data = {}
        # TODO: better conversion of play_head/play_body into dict for play_info!!!
        play_data['play_info']  = raw_data
        play_data['play_date']  = str2date(raw_data['start_date'])
        play_data['play_start'] = str2time(raw_data['start_time'], '%I:%M %p')
        play_data['play_end']   = None # Time
        play_data['play_dur']   = None # Interval
        play_data['notes']      = None # ARRAY(Text)
        play_data['start_time'] = datetimetz(play_data['play_date'], play_data['play_start'], tz)
        play_data['end_time']   = None # TIMESTAMP(timezone=True)
        play_data['duration']   = None # Interval

        rec_data =  {'label'     : raw_data.get('label'),
                     'catalog_no': raw_data.get('catalog_no')}

        entity_str_data = {'composer'  : [raw_data.get('song-composer')],
                           'work'      : [raw_data.get('song-title')],
                           'conductor' : [raw_data.get('song-conductor')],
                           'performers': [raw_data.get('song-soloist soloist-1')],
                           'ensembles' : [raw_data.get('song-orch_ensemble')],
                           'recording' : [],
                           'label'     : [rec_data['label']]}

        return (ml_dict({'play':       play_data,
                         'composer':   {},
                         'work':       {},
                         'conductor':  {},
                         'performers': [],
                         'ensembles':  [],
                         'recording':  rec_data}),
                entity_str_data)

class ParserC24(Parser):
    """Parser for C24 station

    """
    def iter_program_plays(self, playlist):
        """This is the implementation for C24

        :param playlist: Playlist object
        :yield: [tuple] (pl_date, bs4 'div' tag, bs4 'p' tag)
        """
        log.debug("Parsing html for %s", os.path.relpath(playlist.file, playlist.station.station_dir))
        with open(playlist.file) as f:
            soup = BeautifulSoup(f, self.html_parser)

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
            yield (pl_date, prog_div, prog_head)
        return

    def iter_plays(self, prog):
        """This is the implementation for C24

        :param prog: [tuple] yield value from iter_program_plays()
        :yield: bs4 'table' tag
        """
        pl_date, prog_div, prog_head = prog
        play_heads = prog_div.find_next_siblings(['table', 'div'])
        for play_head in play_heads:
            if play_head.name == 'div':
                break
            yield play_head
        return

    def map_program_play(self, prog_info):
        """This is the implementation for C24

        raw data in: [tuple] (pl_date, bs4 'div' tag, bs4 'p' tag)
        normalized data out: {
            'program': {},
            'program_play': {}
        }
        """
        pl_date, prog_div, prog_head = prog_info
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

    def map_play(self, pp_data, play_head):
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
        pp_date = pp_data['prog_play_date']
        start_date = date2str(pp_date)
        start_time = play_start.string.strip()  # "12:01AM"
        raw_data['start_date'] = start_date  # %Y-%m-%d
        raw_data['start_time'] = start_time  # %I:%M%p (12-hour format)
        tz = pytz.timezone(self.station.timezone)

        # Step 2a - try and find label information (<i>...</i> - <a href=...>)
        rec_center = play_body.find(string=re.compile(r'\s+\-\s+$'))
        rec_listing = rec_center.previous_sibling
        # "<label> <cat>" may be absent, in which case rec_listing is an empty <br/> tag
        if rec_listing.string:
            m = re.fullmatch(r'(.*\S) (\w+)', rec_listing.string)
            if m:
                raw_data['label'] = m.group(1)
                raw_data['catalog_no'] = m.group(2)
                processed.add(rec_listing.string)
        rec_buy_url = rec_center.next_sibling
        # Step 2b - get as much info as we can from the "BUY" url
        if rec_buy_url.name == 'a':
            res = urlsplit(rec_buy_url['href'])
            url_fields = parse_qs(res.query, keep_blank_values=True)
            label      = url_fields['label'][0]    if url_fields.get('label') else None
            catalog_no = url_fields['catalog'][0]  if url_fields.get('catalog') else None
            composer   = url_fields['composer'][0] if url_fields.get('composer') else None
            work       = url_fields['work'][0]     if url_fields.get('work') else None
            url_title  = "%s - %s" % (composer, work)
            url_rec    = "%s %s" % (label, catalog_no)
            raw_data['composer'] = composer
            raw_data['work'] = work
            processed.add(url_title)
            if raw_data.get('label') and raw_data.get('catalog_no'):
                if label != raw_data['label'] or catalog_no != raw_data['catalog_no']:
                    log.debug("Recording in URL (%s %s) mismatch with listing (%s %s)" %
                              (label, catalog_no, raw_data['label'], raw_data['catalog_no']))
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
            # REVISIT: this is hacky--the apostrophe matches "oboe d'amore" and the hyphen
            # matches "mezzo-soprano"; need to replace this with real entity recognition!!!
            m = re.fullmatch(r'(.+), ([\w\./ \'-]+)', field.string)
            if m:
                # note, we will let parse_performer_str() determine whether role is conductor,
                # ensemble, etc.
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
                        log.debug("Overwriting work \"%s\" with \"%s\"" %
                                  (raw_data['work'], work))
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

        play_data = {}
        # TODO: better conversion of play_head/play_body into dict for play_info!!!
        play_data['play_info']  = raw_data
        play_data['play_date']  = str2date(raw_data['start_date'])
        play_data['play_start'] = str2time(raw_data['start_time'], '%I:%M%p')
        play_data['play_end']   = None # Time
        play_data['play_dur']   = None # Interval
        play_data['notes']      = None # ARRAY(Text)
        play_data['start_time'] = datetimetz(play_data['play_date'], play_data['play_start'], tz)
        play_data['end_time']   = None # TIMESTAMP(timezone=True)
        play_data['duration']   = None # Interval

        rec_data  = {'label'     : raw_data.get('label'),
                     'catalog_no': raw_data.get('catalog_no')}

        entity_str_data = {'composer'  : [raw_data.get('composer')],
                           'work'      : [raw_data.get('work')],
                           'conductor' : [raw_data.get('conductor')],
                           'performers': [raw_data.get('performer')],
                           'ensembles' : [raw_data.get('ensemble')],
                           'recording' : [],
                           'label'     : [rec_data['label']]}

        return (ml_dict({'play':       play_data,
                         'composer':   {},
                         'work':       {},
                         'conductor':  {},
                         'performers': [],
                         'ensembles':  [],
                         'recording':  rec_data}),
                entity_str_data)

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

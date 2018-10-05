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
from musiclib import MusicLib, COND_STRS, ml_dict
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

# TODO: move this generic parsing logic to musiclib!!!
def mkperf(name, role, orig_str):
    name = name.strip()
    if role:
        role = role.strip()
    if not name:
        log.warn("Empty performer name \"%s\" [%s], parsed from \"%s\"" %
                 (name, role, orig_str))
    elif not re.match(r'\w', name):
        log.warn("Bad leading character in performer \"%s\" [%s], parsed from \"%s\"" %
                 (name, role, orig_str))
    return {'person': {'name'    : name,
                       'raw_name': orig_str if name != orig_str else None},
            'role'  : role}

def mkens(name, orig_str):
    name = name.strip()
    if not name:
        log.warn("Empty ensemble name \"%s\", parsed from \"%s\"" %
                 (name, orig_str))
    elif not re.match(r'\w', name):
        log.warn("Bad leading character in ensemble \"%s\", parsed from \"%s\"" %
                 (name, orig_str))
    return {'name': name}

def parse_performer_str(perf_str, flags = None):
    """
    DESIGN NOTES (for future):
      * context-sensitive application of individual parsing rules, either implicitly
        (e.g. based on station), or explicitly through flags
      * generic parsing using non-alphanum delimiters, entity lookups (refdata), and
        logical entity relationships (either as replacement, or complement)
      * for now, we return performer data only; LATER: need the ability to indicate
        other entities extracted from perf_str!!!

    :param perf_str:
    :param flags: (not yet implemented)
    :return: list of perf_data structures (see LATER above)
    """
    orig_perf_str = perf_str

    def parse_perf_item(perf_item, fld_delim = ','):
        sub_data = []
        if perf_item.count(fld_delim) % 2 == 1:
            fields = perf_item.split(fld_delim)
            while fields:
                pers, role = (fields.pop(0), fields.pop(0))
                # special case for "<ens>/<cond last, first>"
                if pers.count('/') == 1:
                    log.debug("PPS_RULE 1 - slash separating ens from cond_last \"%s\"" % (pers))
                    ens_name, cond_last = pers.split('/')
                    cond_name = "%s %s" % (role, cond_last)
                    sub_data.append(mkperf(ens_name, 'ensemble', orig_perf_str))
                    sub_data.append(mkperf(cond_name, 'conductor', orig_perf_str))
                else:
                    sub_data.append(mkperf(pers, role, orig_perf_str))
        else:
            # TODO: if even number of field delimiters, need to look closer at item
            # contents/format to figure out what to do!!!
            sub_data.append(mkperf(perf_item, None, orig_perf_str))
        return {'performers': sub_data}

    ens_data  = []
    perf_data = []
    ret_data  = ml_dict({'ensembles' : ens_data, 'performers': perf_data})
    # TODO: should really move the quote processing as far upstream as possible (for
    # all fields); NOTE: also need to revisit normalize_* functions in musiclib!!!
    m = re.match(r'"([^"]*)"$', perf_str)
    if m:
        log.debug("PPS_RULE 2 - strip enclosing quotes \"%s\"" % (perf_str))
        perf_str = m.group(1)  # note: could be empty string, handle downstream!
    m = re.match(r'\((.*[^)])\)?$', perf_str)
    if m:
        log.debug("PPS_RULE 3 - strip enclosing parens \"%s\"" % (perf_str))
        perf_str = m.group(1)  # note: could be empty string, handle downstream!
    # special case for ugly record (WNED 2018-09-17)
    m = re.match(r'(.+?)\r', perf_str)
    if m:
        log.debug("PPS_RULE 4 - ugly broken record for WNED \"%s\"" % (perf_str))
        perf_str = m.group(1)
        m = re.match(r'(.+)\[(.+)\],(.+)', perf_str)
        if m:
            perf_str = '; '.join(m.groups())

    if re.match(r'\/.+ \- ', perf_str):
        log.debug("PPS_RULE 5 - leading slash for performer fields \"%s\"" % (perf_str))
        for perf_item in perf_str.split('/'):
            if perf_item:
                ret_data.merge(parse_perf_item(perf_item, ' - '))
    elif ';' in perf_str:
        log.debug("PPS_RULE 6 - semi-colon-deliminted performer fields \"%s\"" % (perf_str))
        for perf_item in perf_str.split(';'):
            if perf_item:
                ret_data.merge(parse_perf_item(perf_item))
    elif perf_str:
        ret_data.merge(parse_perf_item(perf_str))

    return ret_data

def parse_ensemble_str(ens_str, flags = None):
    """
    :param ens_str:
    :param flags: (not yet implemented)
    :return: dict of ens_data/perf_data structures, indexed by type
    """
    orig_ens_str = ens_str

    def parse_ens_item(ens_item, fld_delim = ','):
        sub_ens_data = []
        sub_perf_data = []
        if ens_item.count(fld_delim) % 2 == 1:
            fields = ens_item.split(fld_delim)
            while fields:
                name, role = (fields.pop(0), fields.pop(0))
                # TEMP: if role starts with a capital letter, assume the whole string
                # is an ensemble (though in reality, it may be two--we'll deal with
                # that later, when we have NER), otherwise treat as performer/role!!!
                if re.match(r'[A-Z]', role[0]):
                    sub_ens_data.append(mkens(name, orig_ens_str))
                else:
                    sub_perf_data.append(mkperf(name, role, orig_ens_str))
        else:
            # TODO: if even number of field delimiters, need to look closer at item
            # contents/format to figure out what to do (i.e. NER)!!!
            sub_ens_data.append(mkens(ens_item, orig_ens_str))
        return {'ensembles' : sub_ens_data, 'performers': sub_perf_data}

    def parse_ens_fields(fields):
        sub_ens_data = []
        sub_perf_data = []
        while fields:
            if len(fields) == 1:
                sub_ens_data.append(mkens(fields.pop(0), orig_ens_str))
                break  # same as continue
            # more reliable to do this moving backward from the end (sez me)
            if ' ' not in fields[-1]:
                # REVISIT: we presume a single-word field to be a city/location (for now);
                # as above, we should really look at field contents to properly parse!!!
                ens = ','.join([fields.pop(-2), fields.pop(-1)])
                sub_ens_data.append(mkens(ens, orig_ens_str))
            else:
                # yes, do this twice!
                sub_ens_data.append(mkens(fields.pop(-1), orig_ens_str))
                sub_ens_data.append(mkens(fields.pop(-1), orig_ens_str))
        return {'ensembles' : sub_ens_data, 'performers': sub_perf_data}

    ens_data  = []
    perf_data = []
    ret_data  = ml_dict({'ensembles' : ens_data, 'performers': perf_data})
    if ';' in ens_str:
        for ens_item in ens_str.split(';'):
            if ens_item:
                ret_data.merge(parse_ens_item(ens_item))
    elif ',' in ens_str:
        ens_fields = ens_str.split(',')
        ret_data.merge(parse_ens_fields(ens_fields))
    else:
        # ens_data is implcitly part of ret_data
        ens_data.append(mkens(ens_str, orig_ens_str))

    return ret_data

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

                play_name = "%s - %s" % (play.get('composerName'), play.get('trackName'))
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

        play_info = {}
        play_info['play_info']  = raw_data
        play_info['play_date']  = str2date(sdate, '%m-%d-%Y')
        play_info['play_start'] = str2time(stime)
        play_info['play_end']   = str2time(etime) if etime else None
        play_info['play_dur']   = dt.timedelta(0, 0, 0, dur_msecs) if dur_msecs else None
        play_info['notes']      = None # ARRAY(Text)),
        play_info['start_time'] = None # TIMESTAMP(timezone=True)),
        play_info['end_time']   = None # TIMESTAMP(timezone=True)),
        play_info['duration']   = None # Interval)

        # do the easy stuff first (don't worry about empty records...for now!)
        composer_data   = {'name'      : raw_data.get('composerName')}
        work_data       = {'name'      : raw_data.get('trackName')}
        conductor_data  = {'name'      : raw_data.get('conductor')}
        performers_data = []
        ensembles_data  = []
        recording_data  = {'label'     : raw_data.get('copyright'),
                           'catalog_no': raw_data.get('catalogNumber'),
                           'name'      : raw_data.get('collectionName')}
        entity_str_data = {'composer'  : [composer_data['name']],
                           'work'      : [work_data['name']],
                           'conductor' : [conductor_data['name']],
                           'recording' : [recording_data['name']],
                           'label'     : [recording_data['label']],
                           'performers': [],
                           'ensembles' : []}

        # normalized return structure (build from above elements)
        play_data = ml_dict({'play':       play_info,
                             'composer':   composer_data,
                             'work':       work_data,
                             'conductor':  conductor_data,
                             'performers': performers_data,
                             'ensembles':  ensembles_data,
                             'recording':  recording_data,
                             'entity_str': entity_str_data})

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
        play_info['play_info'] =  raw_data
        play_info['play_date'] =  str2date(raw_data['start_date'])
        play_info['play_start'] = str2time(raw_data['start_time'], '%I:%M %p')
        play_info['play_end'] =   None # Time
        play_info['play_dur'] =   None # Interval
        play_info['notes'] =      None # ARRAY(Text)),
        play_info['start_time'] = None # TIMESTAMP(timezone=True)),
        play_info['end_time'] =   None # TIMESTAMP(timezone=True)),
        play_info['duration'] =   None # Interval)

        composer_data  =  {'name'      : raw_data.get('song-composer')}
        work_data      =  {'name'      : raw_data.get('song-title')}
        conductor_data =  {'name'      : raw_data.get('song-conductor')}
        performers_data = []
        ensembles_data  = []
        recording_data =  {'label'     : raw_data.get('label'),
                           'catalog_no': raw_data.get('catalog_no')}
        entity_str_data = {'composer'  : [composer_data['name']],
                           'work'      : [work_data['name']],
                           'conductor' : [conductor_data['name']],
                           'label'     : [recording_data['label']],
                           'performers': [],
                           'ensembles' : []}

        # normalized return structure (build from above elements)
        play_data = ml_dict({'play':       play_info,
                             'composer':   composer_data,
                             'work':       work_data,
                             'conductor':  conductor_data,
                             'performers': performers_data,
                             'ensembles':  ensembles_data,
                             'recording':  recording_data,
                             'entity_str': entity_str_data})

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
            m = re.match(r'(.+), ([\w\./ ]+)$', field.string)
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
                        log.debug("Overwriting work \"%s\" with \"%s\"" %
                                  (raw_data['work'], work))
                        raw_data['work'] = work
                else:
                    # REVISIT: for now, just assume we have an ensemble name, though we can't
                    # really know what to do on conflict unless/until we parse the contents and
                    # categorize properly (though, could also add both and debug later)!!!
                    if raw_data.get('ensemble'):
                        raise RuntimeError("Can't overwrite ensemble \"%s\" with \"%s\"" %
                                           (raw_data['ensemble'], field.string))
                    raw_data['ensemble'] = field.string

        play_info = {}
        # TODO: better conversion of play_head/play_body into dict for play_info!!!
        play_info['play_info'] =  raw_data
        play_info['play_date'] =  str2date(raw_data['start_date'])
        play_info['play_start'] = str2time(raw_data['start_time'], '%I:%M%p')
        play_info['play_end'] =   None # Time
        play_info['play_dur'] =   None # Interval
        play_info['notes'] =      None # ARRAY(Text)),
        play_info['start_time'] = None # TIMESTAMP(timezone=True)),
        play_info['end_time'] =   None # TIMESTAMP(timezone=True)),
        play_info['duration'] =   None # Interval)

        composer_data   = {'name'      : raw_data.get('composer')}
        work_data       = {'name'      : raw_data.get('work')}
        conductor_data  = {'name'      : raw_data.get('conductor')}
        performers_data = []
        ensembles_data  = []
        recording_data  = {'label'     : raw_data.get('label'),
                           'catalog_no': raw_data.get('catalog_no')}
        entity_str_data = {'composer'  : [composer_data['name']],
                           'work'      : [work_data['name']],
                           'conductor' : [conductor_data['name']],
                           'label'     : [recording_data['label']],
                           'performers': [],
                           'ensembles' : []}

        # normalized return structure (build from above elements)
        play_data = ml_dict({'play':       play_info,
                             'composer':   composer_data,
                             'work':       work_data,
                             'conductor':  conductor_data,
                             'performers': performers_data,
                             'ensembles':  ensembles_data,
                             'recording':  recording_data,
                             'entity_str': entity_str_data})

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

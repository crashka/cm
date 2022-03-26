#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Playlist module
"""

import os.path
import logging
import json
import regex as re
import datetime as dt
from zoneinfo import ZoneInfo
from urllib.parse import urlsplit, parse_qs

from bs4 import BeautifulSoup

from .utils import LOV, prettyprint, str2date, date2str, str2time, time2str, datetimetz
from .core import env, log, dbg_hand, DFLT_HTML_PARSER, ObjCollect
from .musiclib import MusicLib, StringCtx, SKIP_ENS, ml_dict, UNIDENT
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

class Playlist(object):
    """Represents a playlist for a station
    """
    @staticmethod
    def list(sta, ptrn: str = None) -> list:
        """List playlists for a station

        :param sta: station object
        :param ptrn: glob pattern to match (optional trailing '*')
        :return: sorted list of playlist names (same as date)
        """
        return sorted(sta.get_playlists(ptrn))

    def __init__(self, sta, date):
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
        self.status      = PLStatus.NEW
        # TODO: preload trailing hash sequence from previous playlist (or add
        # task to fill the gap as to_do_list item)!!!
        self.hash_seq    = HashSeq()
        self.parse_ctx   = {}
        self.parsed_info = None

    def playlist_info(self, keys = INFO_KEYS, exclude = None):
        """Return playlist info (canonical fields) as a dict comprehension
        """
        if not isinstance(keys, ObjCollect):
            keys = [keys]
        if isinstance(exclude, ObjCollect):
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

    def analyze(self, dryrun = False, force = False):
        """Analyze current playlist using underlying parser

        :param dryrun: don't write to config file
        :param force: overwrite playlist configuration, if exists
        :return: void
        """
        self.parser.analyze(self, dryrun, force)

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
        log.debug("HTML parser: %s" % self.html_parser)
        self.ml = MusicLib()

    def proc_playlist(self, contents: str) -> str:
        """Process raw playlist contents downloaded from URL before saving to file.

        Note that base class implementation does generic processing, so should be called
        by subclass overrides.
        """
        # filter out NULL characters (lost track of which station this is needed for, so
        # we'll just do it generically for now)
        return contents.replace('\u0000', '')

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
                    log.debug("Skipping hash_seq for duplicate play:\n%s" % play_rec)

        return pp_rec

    def analyze(self, playlist, dryrun = False, force = False):
        """Analyze playlist contents, build playlist string parser, write to parser config file if not dryrun

        :param playlist: Playlist object
        :param dryrun: don't write to config file
        :param force: replace (rather than augment) config info
        :return: void
        """
        for prog in self.iter_program_plays(playlist):
            pp_norm = self.map_program_play(prog)
            print("Analyzing program \"%s\":" % (pp_norm['program']['name']))

            parse_patterns = {}
            for play in self.iter_plays(prog):
                play_norm, entity_str_data = self.map_play(pp_norm['program_play'], play)
                for performers_str in entity_str_data['performers']:
                    if performers_str:
                        for pattern in StringCtx.examine_entity_str(performers_str):
                            if not pattern or pattern == UNIDENT:
                                continue
                            if pattern in parse_patterns:
                                parse_patterns[pattern] += 1
                            else:
                                parse_patterns[pattern] = 1

            print("Performer parse patterns for \"%s\":" % (pp_norm['program']['name']))
            prettyprint(parse_patterns)

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
        # INVESTIGATE (at some point): would like to record this, but it looks like
        # this maps to a weekly program identifier, thus different from our current
        # notion of a named program that might run daily!!!
        #prog_data['ext_id'] = prog_info.get('program_id')

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
        tz = ZoneInfo(self.station.timezone)

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

        pp_data['ext_id']          = data.get('_id')
        pp_data['ext_mstr_id']     = data.get('event_id')

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
        tz = ZoneInfo(self.station.timezone)

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

        play_data['ext_id']      = raw_data.get('_id')
        play_data['ext_mstr_id'] = raw_data.get('_source_song_id')

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

#+------------+
#| ParserC24C |
#+------------+

class ParserC24C(Parser):
    def proc_playlist(self, contents: str) -> str:
        """Process raw playlist contents downloaded from URL before saving to file.

        For the C24C/MPR3 implementation, we extract the json data from the html input.
        """
        contents = super().proc_playlist(contents)
        soup = BeautifulSoup(contents, self.html_parser)
        data = soup.find('script', id="__NEXT_DATA__")
        return data.string

    def iter_program_plays(self, playlist: Playlist) -> dict:
        """This is the implementation for C24C and MPR3 (json)

        :param playlist: Playlist object
        :yield: dict representing individual programs
        """
        log.debug("Parsing json for %s", os.path.relpath(playlist.file, playlist.station.station_dir))
        with open(playlist.file) as f:
            pl_info = json.load(f)
        # there may or may not be a 'props' wrapper around 'pageProps', depending on whether
        # the playlist contents were extracted from an HTML page, or downloaded directly as
        # JSON (the latter, for files inherited from the now-obsolete C24B downloader)--the
        # contents should otherwise be processed the same for both cases
        if 'props' in pl_info:
            pl_info = pl_info['props']
        pl_params = pl_info['pageProps']
        pl_data   = pl_params['data']
        pl_hosts  = pl_data['hosts']
        for prog in pl_hosts:
            yield prog

    def iter_plays(self, prog: dict) -> dict:
        """This is the implementation for C24C and MPR3 (json)

        :param prog: dict yield value from iter_program_plays()
        :yield: dict 'songs' item from C24C/MPR3 playlist file
        """
        plays = prog.get('songs') or []
        for play in plays:
            yield play

    def map_program_play(self, prog: dict):
        """This is the implementation for C24C and MPR3 (json)

        raw data in: [list of dicts] 'hosts' item from C24C/MPR3 playlist file
        normalized data out: {
            'program': {},
            'program_play': {}
        }
        """
        host_name = prog['hostName']
        show_name = prog['showName']
        prog_data = {'name': f"{show_name} with {host_name}"}

        prog.get('showLink')   # "http://minnesota.publicradio.org/radio/services/cms/"
        prog.get('startTime')  # "2022-03-02T00:00:00-06:00"
        prog.get('endTime')    # "2022-03-02T06:00:00-06:00"
        prog.get('id')         # 702873

        start_dt = dt.datetime.fromisoformat(prog['startTime'])
        end_dt = dt.datetime.fromisoformat(prog['endTime'])

        pp_data = {}
        pp_data['prog_play_info']  = prog
        pp_data['prog_play_date']  = start_dt.date()
        pp_data['prog_play_start'] = start_dt.time()
        pp_data['prog_play_end']   = end_dt.time()
        pp_data['prog_play_dur']   = None # Interval, if listed
        pp_data['notes']           = None # ARRAY(Text)
        pp_data['start_time']      = start_dt
        pp_data['end_time']        = end_dt
        pp_data['duration']        = pp_data['end_time'] - pp_data['start_time']

        pp_data['ext_id']          = prog.get('id')
        pp_data['ext_mstr_id']     = None

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
        start_dt = dt.datetime.fromisoformat(raw_data['played_at'])
        end_dt = dt.datetime.fromisoformat(raw_data['ended_at'])
        if dur_str := raw_data.get('duration'):
            min_str, sec_str = dur_str.split(':')
            play_dur = dt.timedelta(minutes=int(min_str), seconds=int(sec_str))
        else:
            play_dur = None

        tz = ZoneInfo(self.station.timezone)

        play_data = {}
        play_data['play_info']   = raw_data
        play_data['play_date']   = start_dt.date()
        play_data['play_start']  = start_dt.time()
        play_data['play_end']    = end_dt.time()
        play_data['play_dur']    = play_dur
        play_data['notes']       = None # ARRAY(Text)
        play_data['start_time']  = start_dt
        play_data['end_time']    = end_dt
        play_data['duration']    = play_data['end_time'] - play_data['start_time']

        play_data['ext_id']      = raw_data.get('play_id')
        play_data['ext_mstr_id'] = raw_data.get('song_id')

        rec_data = {'name'      : raw_data.get('album'),
                    'label'     : raw_data.get('record_co'),
                    'catalog_no': raw_data.get('record_id')}

        perf_keys = (f"soloist_{n}" for n in range(1, 7))
        perf_iter = filter(None, (raw_data.get(k) for k in perf_keys))

        entity_str_data = {'composer'  : [raw_data.get('composer')],
                           'work'      : [raw_data.get('title')],
                           'conductor' : [raw_data.get('conductor')],
                           'performers': list(perf_iter),
                           'ensembles' : [raw_data.get('orch_ensemble')],
                           'recording' : [rec_data['name']],
                           'label'     : [rec_data['label']]}

        return (ml_dict({'play'      : play_data,
                         'composer'  : {},
                         'work'      : {},
                         'conductor' : {},
                         'performers': [],
                         'ensembles' : [],
                         'recording' : rec_data}),
                entity_str_data)

#+------------+
#| ParserKUSC |
#+------------+

class ParserKUSC(Parser):
    """Parser for KUSC station
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
            raise RuntimeError("Could not parse title \"%s\"" % title)
        pl_date = dt.datetime.strptime(m.group(1), '%B %d, %Y').date()

        pl_root = soup.find('dl', id="playlist")
        for prog_head in reversed(pl_root('dt', recursive=False)):
            yield pl_date, prog_head
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
        tz         = ZoneInfo(self.station.timezone)
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
        tz = ZoneInfo(self.station.timezone)

        buy_button = play_head.find('a', class_="buy-button", href=True)
        if buy_button:
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

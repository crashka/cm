#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Playlist module
"""

from os.path import relpath
import logging
import json
import regex as re
import datetime as dt
from zoneinfo import ZoneInfo
from urllib.parse import urlsplit, parse_qs

from bs4 import BeautifulSoup, Tag

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

def is_name_suffix(ent_str: str) -> bool:
    """Quick and dirty determination whether the given entity string is actually
    actually a name suffix ("Jr", "Sr", others???)

    TEMP: this should be replaced by more generic name/string processing, shared
    with the MusicLib module (see `parse_person_str()` over there)!!!
    """
    # don't do `fullmatch` just in case there is some random noise (for now...)
    if m := re.match(r'((?:Jr|Sr)\.?)(?:\W|$)', ent_str.strip(), flags=re.I):
        suffix = m.group(1)  # for debugging
    return bool(m)

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
        parsed_info = []

        for prog_play in self.iter_program_plays(playlist):
            # Step 1 - Parse out program_play info
            pp_norm = self.map_program_play(prog_play)
            pp_rec = self.ml.insert_program_play(playlist, pp_norm)
            if not pp_rec:
                raise RuntimeError("Could not insert program_play")
            playlist.parse_ctx['station_id']   = pp_rec['station_id']
            playlist.parse_ctx['prog_play_id'] = pp_rec['id']
            playlist.parse_ctx['play_id']      = None
            pp_rec['plays'] = []

            # Step 2 - Parse out play info (if present)
            for play in self.iter_plays(prog_play):
                play_norm, entity_str_data = self.map_play(pp_norm['program_play'], play)
                if not play_norm:
                    continue
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

            # for prog_plays (after adding plays)...
            parsed_info.append(pp_rec)

        return parsed_info

    def analyze(self, playlist, dryrun = False, force = False):
        """Analyze playlist contents, build playlist string parser, write to parser config file if not dryrun

        :param playlist: Playlist object
        :param dryrun: don't write to config file
        :param force: replace (rather than augment) config info
        :return: void
        """
        for prog_play in self.iter_program_plays(playlist):
            pp_norm = self.map_program_play(prog_play)
            print("Analyzing program \"%s\":" % (pp_norm['program']['name']))

            parse_patterns = {}
            for play in self.iter_plays(prog_play):
                play_norm, entity_str_data = self.map_play(pp_norm['program_play'], play)
                if not play_norm:
                    continue
                # see APOLOGY in `parse()` above
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
        log.debug("Parsing json for %s", relpath(playlist.file, playlist.station.station_dir))
        with open(playlist.file) as f:
            pl_info = json.load(f)
        pl_params = pl_info['params']
        pl_progs = pl_info['onToday']
        for prog_play in pl_progs:
            yield prog_play
        return

    def iter_plays(self, prog_play):
        """This is the implementation for WWFM (and others)

        :param prog_play: [list of dicts] yield value from iter_program_plays()
        :yield: 'playlist' item from WWFM playlist file
        """
        plays = prog_play.get('playlist') or []
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
        log.debug("Parsing json for %s", relpath(playlist.file, playlist.station.station_dir))
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
        for prog_play in pl_hosts:
            yield prog_play

    def iter_plays(self, prog_play: dict) -> dict:
        """This is the implementation for C24C and MPR3 (json)

        :param prog_play: dict yield value from iter_program_plays()
        :yield: dict 'songs' item from C24C/MPR3 playlist file
        """
        plays = prog_play.get('songs') or []
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
        pp_data['prog_play_info']  = {k: v for k, v in prog.items() if k != 'songs'}
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
        """This is the implementation for C24C and MPR3 (json)

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
        """This is the implementation for KUSC

        :param playlist: Playlist object
        :yield: [tuple] (pl_date, bs4 'div' tag)
        """
        def norm_date(s: str) -> str:
            """Normalize date string by cardinalizing the day of month and
            removing redundant day of week ("Wednesday March 2nd 2022" ->
            "March 2 2022")
            """
            card = re.sub(r'(\d)(st|nd|rd|th)', r'\1', s)
            return card.split(' ', 1)[1]

        log.debug("Parsing html for %s", relpath(playlist.file, playlist.station.station_dir))
        with open(playlist.file) as f:
            soup = BeautifulSoup(f, self.html_parser)

        pl = soup.find('div', class_="accordion")
        pl_hdr = pl.find('div', class_="accordion-top")
        datestr = next(pl_hdr.h3.stripped_strings)
        datestr_norm = norm_date(datestr)
        pl_date = dt.datetime.strptime(datestr_norm, "%B %d %Y").date()

        pl_root = pl.find('div', id="accordion-playlist-wrapper")
        for prog_wrapper in pl_root('div', class_="accordion-section", recursive=False):
            yield pl_date, prog_wrapper
        return

    def iter_plays(self, prog_play):
        """This is the implementation for KUSC

        :param prog_play: [tuple] yield value from iter_program_plays()
        :yield: bs4 'tr' tag
        """
        pl_date, prog_wrapper = prog_play
        prog_body = prog_wrapper.find('div', class_="accordion-body")

        for play_head in prog_body.table.tbody('tr', recursive=False):
            # missing `th` indicates empty play item, but there may be some annotation
            # inside the `tr` (e.g. "Song data not yet available for this segment"), so
            # let's log what we can find
            if play_head.th is None:
                notes = ' | '.join(play_head.stripped_strings)
                log.notice(f"Skipping empty play item for {pl_date.strftime('%Y-%m-%d')} "
                           f"'{notes}', full html...\n{str(prog_wrapper)}")
                continue
            yield play_head
        return

    def map_program_play(self, prog_play):
        """This is the implementation for KUSC

        raw data in: [tuple] (pl_date, bs4 'div' tag)
        normalized data out: {
            'program': {},
            'program_play': {}
        }
        """
        tz = ZoneInfo(self.station.timezone)

        pl_date, prog_wrapper = prog_play
        prog_head = prog_wrapper.find('div', class_="accordion-content")

        prog_name = ' '.join(prog_head.h3.stripped_strings)
        prog_data = {'name': prog_name}

        start, end = prog_head.h4('span', class_="actual-utime")
        # ATTENTION: the following two lines do not work, since these unixtime values
        # are not correct (representing the right time, but a fixed date in the week),
        # so we need to construct `start_dt` and `end_dt` using `pl_date`!!!
        #start_dt = dt.datetime.fromtimestamp(int(start['data-unixtime']), tz=tz)
        #end_dt = dt.datetime.fromtimestamp(int(end['data-unixtime']), tz=tz)
        start_time = dt.datetime.strptime(start.string, "%I%p")
        end_time   = dt.datetime.strptime(end.string, "%I%p")
        start_dt   = dt.datetime.combine(pl_date, start_time.time(), tz)
        end_dt     = dt.datetime.combine(pl_date, end_time.time(), tz)

        pp_data = {}
        # TODO: convert prog_head into dict for prog_play_info!!!
        pp_data['prog_play_info']  = {'html': str(prog_wrapper)}
        pp_data['prog_play_date']  = start_dt.date()
        pp_data['prog_play_start'] = start_dt.time()
        pp_data['prog_play_end']   = end_dt.time()
        pp_data['prog_play_dur']   = None # Interval, if listed
        pp_data['notes']           = None # ARRAY(Text)
        pp_data['start_time']      = start_dt
        pp_data['end_time']        = end_dt
        pp_data['duration']        = pp_data['end_time'] - pp_data['start_time']

        pp_data['ext_id']          = None
        pp_data['ext_mstr_id']     = None

        return {'program': prog_data, 'program_play': pp_data}

    def map_play(self, pp_data, play):
        """This is the implementation for KUSC

        raw data in: bs4 'tr' tag
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
        tz = ZoneInfo(self.station.timezone)

        # TODO: compute end_ts based on next play (or previous play, depending on
        # which order we process the items)!!!
        start_ts = int(play.th['data-unixtime'])
        start_dt = dt.datetime.fromtimestamp(start_ts, tz=tz)
        start_time = play.th.string.strip()  # redundant info, can ignore

        fields = play('td')
        # LATER: validate fields against columns names in the program play header,
        # i.e. `prog_body.table.thead.('th')`!!!
        #   title_td      - fields[0]
        #   composer_td   - fields[1]
        #   performers_td - fields[2]
        #   record_td     - fields[3]
        #   buy_cd_td     - fields[4]

        # title and composer fields seem to always be simple strings, but let's check
        # for anything different, so we know how to treat the complex case properly
        if len(fields[0].contents) == 1:
            title = fields[0].string.strip()
        else:
            title = ' '.join(fields[0].stripped_strings)
            log.notice(f"Complex value found for title: '{fields[0]}'")

        if len(fields[1].contents) == 1:
            composer = fields[1].string.strip()
        else:
            composer = ' '.join(fields[1].stripped_strings)
            log.notice(f"Complex value found for composer: '{fields[1]}'")

        # performers field seems to be one or two navigable strings, but we'll handle
        # the case of N > 2 also.  ASSUMPTION: if there are multiple strings, the last
        # one will always contain the "conductor / orchestra" pattern!!!  Note that we
        # are fine if this isn't the case, but good to validate, just so we know.
        #
        # String patterns (within navigable strings):
        #   - Soloist, Instrument
        #   - Soloist, Instrument; Soloist, Instument; ...
        #   - Conductor / Orchestra
        #   - Ensemble
        perf_list = []
        cond_list = []
        ens_list  = []

        lines = 0
        for perf_line in fields[2].stripped_strings:
            lines += 1
            for perf_item in (x.strip() for x in perf_line.split(';')):
                # check for "Conductor / Orchestra"
                item_segs = perf_item.split(' / ')
                if len(item_segs) > 1:
                    if len(item_segs) > 2:
                        log.debug(f"UNEXPECTED: multiple slash delimiters in '{perf_item}'")
                        item_segs = perf_item.split(' / ', 1)  # resplit with limit
                    cond_list.append(item_segs[0])
                    ens_list.append(item_segs[1])
                    continue

                # check for "Soloist, Instrument" - need to make sure that commas are not
                # being used for other purposes (e.g. name suffixes)
                item_segs = perf_item.rsplit(',', 1)
                if len(item_segs) > 1 and not is_name_suffix(item_segs[1]):
                    # TODO: find a way to push this accurate parsing downstream, but for now
                    # we just let the downstream code figure this out!!!
                    soloist, instr = item_segs  # not stripped!!!
                    perf_list.append(perf_item)
                    continue

                # treat all other cases as an ensemble name
                ens_list.append(perf_item)
        # finally...
        if lines > 2:
            log.debug(f"UNEXPECTED: more than 2 lines in performer string '{str(fields[2])}'")

        # recording information is composed of separate strings for label and catalog number,
        # with the latter being optional
        rec_info = fields[3].stripped_strings

        play_data = {}
        play_data['play_info']   = {'html': str(play)}
        play_data['play_date']   = start_dt.date()
        play_data['play_start']  = start_dt.time()
        play_data['play_end']    = None
        play_data['play_dur']    = None
        play_data['notes']       = None # ARRAY(Text)
        play_data['start_time']  = start_dt
        play_data['end_time']    = None
        play_data['duration']    = None

        play_data['ext_id']      = None
        play_data['ext_mstr_id'] = None

        rec_data = {'name'      : None,
                    'label'     : next(rec_info, None),
                    'catalog_no': next(rec_info, None)}

        entity_str_data = {'composer'  : [composer],
                           'work'      : [title],
                           'conductor' : cond_list,
                           'performers': perf_list,
                           'ensembles' : ens_list,
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
#| ParserWDAV |
#+------------+

class ParserWDAV(Parser):
    """Parser for WDAV station

    Notes:
      - This station does not do a good job of inlining the program information,
        so we will skip it for now (probably not worth revisting even)
    """
    DFLT_PROG_NAME = "Classical Music"

    ProgPlay = tuple[dt.datetime, Tag]
    Play     = tuple[Tag, Tag]

    def iter_program_plays(self, playlist: Playlist) -> ProgPlay:
        """Iterator for program plays within a playlist, yield value is passed into
        map_program_play() and iter_plays())

        - Playlist date ("%b %d, %Y") can be found in the following section:
            <section class="dark-bg" id="title" style="position: fixed; [...]">
              <div class="container" style="height: 35px;">
                <div class="col-sm-12">
                  <h4 style="line-height: 35px;">
                    WDAV Playlist for Apr 25, 2022
                  </h4>
                </div>
              </div>
            </section>
      - Playlist starts with `<section id="nowplaying">`

        :param playlist: Playlist object
        :yield: subclass-specific program play representation
        """
        log.debug("Parsing html for %s", relpath(playlist.file, playlist.station.station_dir))
        with open(playlist.file) as f:
            soup = BeautifulSoup(f, self.html_parser)

        title_str = soup.find('section', id="title").h4.string
        if m := re.match(r'WDAV Playlist for (.+)', title_str):
            date_str = m.group(1).strip()
            pl_date = dt.datetime.strptime(date_str, "%b %d, %Y")
        else:
            # TODO: get date from file name (if needed)!!!
            assert False
        # as stated above, this station does a crappy job at delineating programs, so we just
        # return the entire playlist wrapper as a single program play
        prog_wrapper = soup.find('section', id="nowplaying")
        yield pl_date, prog_wrapper
        return

    def iter_plays(self, prog_play: ProgPlay) -> Play:
        """Iterator for plays within a program play, yield value is passed into map_play()

        :param prog_play: yield value from iter_program_plays()
        :yield: Play
        """
        pl_date, prog_wrapper = prog_play

        for play_root in prog_wrapper('div', class_="container", recursive=False):
            time_divs = play_root('div', class_="col-sm-2")
            play_divs = play_root('div', class_="col-sm-10")
            if time_divs and play_divs:
                yield time_divs[0], play_divs[0]
        return

    def map_program_play(self, prog_play: ProgPlay) -> dict:
        """
        :param prog_play: yield value from iter_program_plays()
        :return: dict of normalized program play information
        """
        tz = ZoneInfo(self.station.timezone)

        pl_date, prog_wrapper = prog_play

        prog_name = self.DFLT_PROG_NAME
        prog_data = {'name': prog_name}

        start_dt = dt.datetime.combine(pl_date, dt.time(0, 0), tz)
        end_dt   = start_dt + dt.timedelta(days=1)

        pp_data = {}
        # TODO: convert prog_head into dict for prog_play_info!!!
        pp_data['prog_play_info']  = {'html': str(prog_wrapper)}
        pp_data['prog_play_date']  = start_dt.date()
        pp_data['prog_play_start'] = start_dt.time()
        pp_data['prog_play_end']   = end_dt.time()
        pp_data['prog_play_dur']   = None # Interval, if listed
        pp_data['notes']           = None # ARRAY(Text)
        pp_data['start_time']      = start_dt
        pp_data['end_time']        = end_dt
        pp_data['duration']        = pp_data['end_time'] - pp_data['start_time']

        pp_data['ext_id']          = None
        pp_data['ext_mstr_id']     = None

        return {'program': prog_data, 'program_play': pp_data}

    def map_play(self, pp_data, play):
        """
        Each play item is a child `<div>` that looks like this:
          <div class="container" style="border-bottom: 1px dotted #cccccc; [...]">
            <div class="col-sm-2">8:01 PM</div>
            <div class="col-sm-10">
              <b>Franz Schubert:</b>
              <b><i>Octet in F, D.803 (1st-3rd mvts.) </i></b>
              <p>Gidon Kremer, Isabelle van Keulen, violins; Tabea Zimmermann, viola; [...]<br></p>
              <p><i>Deutsche Grammophon 423367     "Schubert: Octet"</i></p>
              <form method="post" action="https://www.arkivmusic.com/[...]" target="_blank">
                <input type="submit" value="Buy It" class="buyitbutton" [...] />
              </form>
            </div>
          </div>

        Within the play info (div "col-sm-10"):
          - One or two <p> tags (no <i>), second one is always(?) "Conducted by [...]"
          - <p> elements always terminated with <br>
          - <br> within <p> used to separate performers from ensemble
          - <p><i> always(?) indicates "Label Cat_No/Title"
          - Note that the performer field can be truncated, therefore incomplete and
            potentially not properly parsable (sucks!)

        :param pp_data: [dict] parent program play data (from map_program_play())
        :param play: yield value from iter_plays()
        :return: dict of normalized play information
        """
        tz = ZoneInfo(self.station.timezone)

        time_div, play_div = play
        play_date = pp_data['prog_play_date']
        play_time = dt.datetime.strptime(time_div.string, "%I:%M %p").time()
        start_dt  = dt.datetime.combine(play_date, play_time, tz)

        """
        <b>Franz Schubert:</b>
        <b><i>Octet in F, D.803 (1st-3rd mvts.) </i></b>
        <p>Gidon Kremer, Isabelle van Keulen, violins; Tabea Zimmermann, viola; [...]<br></p>
        <p><i>Deutsche Grammophon 423367     "Schubert: Octet"</i></p>
        """
        comp_list = []
        work_list = []
        perf_list = []
        cond_list = []
        ens_list  = []
        rec_name  = None
        label     = None
        cat_no    = None

        # composer / work / program(?)
        for item in play_div('b'):
            if item.i:
                work_list.append(item.i.string.strip())
            elif item.string[-1] == ':':
                comp_list.append(item.string[:-1].strip())
            else:
                # If no trailing colon, this is probably the program name, which we are not
                # handling for now (or ever?)--for now, we'll just identify it locally and
                # return an empty record to indicate this is not a play.  Note that program
                # info may be contatined within a <p> and/or as a bare NavigableString under
                # `play_div`
                program_name = item.string.strip()
                return None, None

        # performer(s) / conductor / recording
        def parse_rec_info(rec_info: str) -> tuple[str, str]:
            """Parse out label and catalog number from combined string, we split on whitespace
            and make interpretations on what the various segments mean based on how "catalogy-
            looking" they are
            """
            segs = re.split(r'\s+', rec_info)
            if len(segs) == 1:
                return rec_info, None
            elif not re.fullmatch(r'[0-9.-]', segs[-1]):  # last segment doesn't look catalogy
                return rec_info, None
            elif len(segs) == 2:
                # note that the logic below would actually handle this correctly, but
                # we'lll make it explicit here, since it is the most common case
                return segs[0], segs[1]

            # otherwise we assume all trailing segments that look catalogy comprise the
            # catalog number (note, we already know the last segment qualifies, but we
            # include it in the iteration to keep the logic simpler)
            for i, seg in enumerate(segs[::-1]):
                if not re.fullmatch(r'[0-9.-]', seg):
                    return ' '.join(segs[:-i]), ' '.join(segs[-i:])

            # not quite sure what to do now, but we'll go with treating the last segment
            # as the catalog number, and the rest as the label name
            return ' '.join(segs[:-1]), ' '.join(segs[-1:])

        for item in play_div('p'):
            if item.i:
                # record information is uniquely in italics
                if m := re.match(r'([^"]+)"([^"]+)"', item.i.string):
                    rec_info = m.group(1).strip()
                    rec_name = m.group(2).strip()
                    label, cat_no = parse_rec_info(rec_info)
                else:
                    # assume there is no quoted album title
                    label, cat_no = parse_rec_info(item.i.string)
                # MAYBE assert that this is the last item (in which case we could
                # break instead, for dramatic value, haha)???
                continue

            # processing logic for other play information:
            # - multiple strings implies soloist(s) and ensemble (separated by
            #   <br> in the html)
            # - individual strings parse as follows:
            #   - parse out annotation for live performances (e.g."(Live in Vienna,
            #     July 7, 1990)"), sometime embedded in ensemble or performer string;
            #     as an added benefit this also takes misleading commas out of play
            #   - multiple performers are delimited by semi-colons
            #   - performer name and role are delimited by commas (noting that roles
            #     are typically lower-case, which we can use to distinguish from name
            #     suffixes)
            #   - conductors are indicated as "Conducted by ..."
            #   - ensemble names may actually have a comma (e.g. indicating a place
            #     name), so we need to distinguish between this case and a performer
            #     with a name suffix
            for item_str in item.stripped_strings:
                # note that closing paren may be missing due to pesky truncation
                if m := re.match(r'(.+)(\(Live [^\)]+(?:\)|$))(.*)', item_str):
                    notes = m.group(2)[1:]
                    # don't add space separator, since parenthesized note (extracted)
                    # will typically have some white space around it
                    item_str = m.group(1) + m.group(3)

                if m := re.match(r'Conducted by (.+)', item_str):
                    cond_list.append(m.group(1).strip())
                elif ';' in item_str:
                    # we *know* this is a list of performers, so we don't have to worry
                    # about misdirection from spurious commas in ensemble names; it may
                    # may not be worth actually handling specially/separately
                    for perf_item in (x.strip() for x in item_str.split(';')):
                        item_segs = perf_item.rsplit(',', 1)
                        if len(item_segs) > 1 and item_segs[1] and item_segs[1].strip()[0].islower():
                            # TODO: find a way to push this accurate parsing downstream,
                            # see similar code in ParserKUSC!!!
                            soloist, instr = item_segs  # not stripped!!!
                            perf_list.append(perf_item)
                        else:
                            # let downstream code make sense of this performer string
                            perf_list.append(perf_item)
                else:
                    item_segs = item_str.rsplit(',', 1)
                    if len(item_segs) > 1 and item_segs[1] and item_segs[1].strip()[0].islower():
                        # TODO: see TODO above!!!
                        soloist, instr = item_segs  # not stripped!!!
                        perf_list.append(item_str)
                        continue

                    # Note that we are not able to identify instances of a bare performer
                    # string (unlike the multiple-performer case above)--if it even really
                    # happens--without NER techniques, so for now we treat all other cases
                    # as an ensemble name
                    ens_list.append(item_str)

        play_data = {}
        play_data['play_info']   = {'html': str(play_div)}
        play_data['play_date']   = start_dt.date()
        play_data['play_start']  = start_dt.time()
        play_data['play_end']    = None
        play_data['play_dur']    = None
        play_data['notes']       = None # ARRAY(Text)
        play_data['start_time']  = start_dt
        play_data['end_time']    = None
        play_data['duration']    = None

        play_data['ext_id']      = None
        play_data['ext_mstr_id'] = None

        rec_data = {'name'      : rec_name,
                    'label'     : label,
                    'catalog_no': cat_no}

        entity_str_data = {'composer'  : comp_list,
                           'work'      : work_list,
                           'conductor' : cond_list,
                           'performers': perf_list,
                           'ensembles' : ens_list,
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
#| ParserWQXR |
#+------------+

class ParserWQXR(Parser):
    """Parser for WQXR station
    """
    pass

#+------------+
#| ParserWFMT |
#+------------+

class ParserWFMT(Parser):
    """Parser for WFMT station
    """
    pass

#+------------+
#| ParserKING |
#+------------+

class ParserKING(Parser):
    """Parser for KING station
    """
    pass

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

# -*- coding: utf-8 -*-

"""ParserWWFM subclass implementation
"""

import json
import datetime as dt
from zoneinfo import ZoneInfo

from .base import Parser
from ..utils import str2date, str2time, datetimetz
from ..core import log
from ..musiclib import ml_dict

##############
# ParserWWFM #
##############

class ParserWWFM(Parser):
    """Parser for WWFM-family of stations
    """
    def iter_program_plays(self, playlist):
        """This is the implementation for WWFM (and others)

        :param playlist: Playlist object
        :yield: [list of dicts] 'onToday' item from WWFM playlist file
        """
        log.debug(f"Parsing json for {playlist.rel_path}")
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
            edate, etime = None, None

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

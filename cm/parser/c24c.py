# -*- coding: utf-8 -*-

"""ParserC24C subclass implementation
"""

import json
import datetime as dt

from bs4 import BeautifulSoup

from .base import Parser
from ..core import log
from ..playlist import Playlist
from ..musiclib import ml_dict

##############
# ParserC24C #
##############

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
        log.debug(f"Parsing json for {playlist.rel_path}")
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

    def map_play(self, pp_norm, raw_data):
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

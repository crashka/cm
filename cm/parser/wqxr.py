# -*- coding: utf-8 -*-

"""ParserWQXR subclass implementation
"""

import datetime as dt
from typing import NewType
from urllib.parse import urlparse, parse_qs

import json
import regex as re
from bs4 import BeautifulSoup, NavigableString

from .base import Parser
from ..core import log
from ..utils import str2date, str2time, str2dur, datetimetz
from ..playlist import Playlist
from ..musiclib import ml_dict

#######################
# Constants/Functions #
#######################

DFLT_TIME_FMT = "%I:%M %p"

def str2dur_legacy(dur_str: str) -> dt.timedelta | None:
    """Parse legacy duration representation, pattern: 1 hrs 15 min 58 s, where "hrs"
    and "min" may or may not be present
    """
    if m := re.fullmatch(r'(?:([0-9]+) hrs )?(?:([0-9]+) min )?([0-9]+) s', dur_str):
        hrs, mins, secs = m.groups()
        return dt.timedelta(hours=int(hrs or 0), minutes=int(mins or 0), seconds=int(secs))

    return None

##############
# ParserWQXR #
##############

ProgPlay = NewType('ProgPlay', dict)
Play     = NewType('Play', dict)

class ParserWQXR(Parser):
    """Parser for WQXR station
    """
    def iter_program_plays(self, playlist: Playlist) -> ProgPlay:
        """Iterator for program plays within a playlist, yield value is passed into
        map_program_play() and iter_plays())

        :param playlist: Playlist object
        :yield: subclass-specific program play representation
        """
        log.debug(f"Parsing json for {playlist.rel_path}")
        with open(playlist.file) as f:
            pl_info = json.load(f)
        pl_events = pl_info['events']
        for prog_play in pl_events:
            #if prog_play.get('event_title') is None:
            if not prog_play.get('playlists'):  # not exists or empty list
                continue
            # not sure what this ID represents, but let's pull it out anyway
            prog_play['pl_id'] = prog_play['playlists'][0]['id']
            prog_play['date'] = playlist.datestr
            yield prog_play
        return

    def iter_plays(self, prog_play: ProgPlay) -> Play:
        """Iterator for plays within a program play, yield value is passed into map_play()

        :param prog_play: yield value from iter_program_plays()
        :yield: Play
        """
        pp_cont = prog_play['playlists'][0]

        for play in pp_cont['played']:
            yield play
        return

    def map_program_play(self, prog_play: ProgPlay) -> dict:
        """
        :param prog_play: yield value from iter_program_plays()
        :return: dict of normalized program play information

        Notes:
          - `prog_play` looks like this:
              {
                "id"                 : "event_1200AM",
                "show_id"            : 1125633,
                "object_id"          : 1125633,
                "event_title"        : "New York At Night with Lauren Rico",
                "pl_id"              : "playlist_109557",

                "iso_start_timestamp": "2022-11-15T05:00:00+00:00",
                "iso_end_timestamp"  : "2022-11-15T10:30:00+00:00",

                "start_timestamp"    : "2022-11-15T00:00:00",
                "end_timestamp"      : "2022-11-15T05:30:00",

                "date"               : "2022-11-15",
                "time"               : "12:00 AM",
                "starttime"          : "12:00",
                "endtime"            : "05:30 AM",

                "playlists"          : [ {...} ]
              }
        """
        def get_notes(prog_play: ProgPlay) -> list | None:
            """Extract notes from teaser field(s) in `prog_play`
            """
            notes = []
            if tease_html := prog_play.get('scheduletease'):
                soup = BeautifulSoup(tease_html, 'lxml')
                text_div = soup.find('div', class_="text")
                if title := text_div.find('a', recursive=False):
                    notes.append(title.string.strip())
                if hosts_ul := text_div.find('ul', class_="hosts"):
                    notes.append(' '.join(hosts_ul.stripped_strings))
                if tease_div := text_div.find('div', class_="tease"):
                    notes.append(' '.join(tease_div.stripped_strings))
            return notes or None

        prog_data = {'name': prog_play['event_title'], 'notes': get_notes(prog_play)}

        sdate, stime = prog_play['start_timestamp'].split('T')
        edate, etime = prog_play['end_timestamp'].split('T')
        if sdate != prog_play['date']:
            log.debug(f"Date mismatch {sdate} != {prog_play['date']}")

        pp_data = {}
        pp_data['prog_play_info']  = prog_play
        pp_data['prog_play_date']  = str2date(sdate)
        pp_data['prog_play_start'] = str2time(stime)
        pp_data['prog_play_end']   = str2time(etime)
        pp_data['prog_play_dur']   = None # Interval, if listed
        pp_data['notes']           = None # ARRAY(text)
        pp_data['start_time']      = datetimetz(sdate, stime, self.tz)
        pp_data['end_time']        = datetimetz(edate, etime, self.tz)
        pp_data['duration']        = pp_data['end_time'] - pp_data['start_time']

        pp_data['ext_id']          = prog_play.get('pl_id')
        pp_data['ext_mstr_id']     = prog_play.get('show_id')

        return {'program': prog_data, 'program_play': pp_data}

    def map_play(self, pp_norm: dict, play: Play) -> tuple[ml_dict, dict]:
        """This is the implementation for WQXR (and others)

        raw data in: 'playlist' item from WQXR playlist file
        normalized data out: {
            'composer'  : {},
            'work'      : {},
            'conductor' : {},
            'performers': [{}, ...],
            'ensembles' : [{}, ...],
            'recording' : {},
            'play'      : {}
        }

        Notes:
          - `play` looks like this:
              {
                "iso_start_time": "2022-12-08T18:02:30+00:00",
                "info"          : "<div class=\"piece-info\"> ... </div>",
                "id"            : "entry_2493151",
                "time"          : "01:02 PM"
              }
          - the ISO timestamp above is not accurate (date is wrong), so we have to compute
            the actual start time using the playlist date and the `time` field here
        """
        pp_data = pp_norm['program_play']
        play_date = pp_data['prog_play_date']
        play_start = str2time(play['time'], DFLT_TIME_FMT)
        play_duration = None

        soup = BeautifulSoup(play['info'], self.html_parser)
        info_div = soup.find('div', class_="piece-info")
        if duration_div := soup.find('div', class_="playlist-item__duration"):
            dur_str = duration_div.string.strip()
            play_duration = str2dur(dur_str) or str2dur_legacy(dur_str)

        comp_list = []
        work_list = []
        perf_list = []
        cond_list = []
        ens_list  = []
        rec_name  = None
        label     = None
        cat_no    = None

        for item in info_div.ul('li'):
            # TODO: replace all asserts herein with exception checking/logging!!!
            item_a = item.a
            if 'class' not in item.attrs:
                if item_a:
                    assert 'class' in item_a.attrs
                    assert "playlist-item__composer" in item_a['class']
                    composer = item_a.string.strip()
                    composer_link = item_a['href']
                    comp_list.append(composer)
                    # TODO: do something iwth `composer_link`!!!
                else:
                    # older playlists (before 2019-02-22) have legacy duration format
                    # in a naked <li> element
                    assert play_duration is None
                    play_duration = str2dur_legacy(item.string.strip())
            elif "playlist-item__title" in item['class']:
                assert not item_a
                title = item.string.strip()
                work_list.append(title)
            elif "playlist-item__musicians" in item['class']:
                assert item_a
                perf = item_a.string.strip()
                perf_link = item_a['href']
                assert isinstance(item_a.next_sibling, NavigableString)
                next_str = item_a.next_sibling.strip()
                if next_str[:2] == ", ":
                    role = next_str[2:]
                    if role == 'conductor':
                        cond_list.append(perf)
                    else:
                        perf_list.append(perf + next_str)
                else:
                    ens_list.append(perf)
            else:
                # unexpected item!!!
                assert False

        album_div = info_div.find('div', class_="album-info")
        if actions := album_div.find('ul', class_="playlist-actions"):
            for action in actions('li'):
                if "playlist-item__album" in action['class']:
                    rec_name = action.string.strip()
                elif "playlist-buy" in action['class']:
                    assert action.a
                    res = urlparse(action.a['href'])
                    flds = parse_qs(res.query)
                    # {'cat': ['72'], 'id': ['127424'], 'label': ['Dynamic']}
                    if 'label' in flds:
                        label = flds['label'][0]
                    if 'cat' in flds:
                        cat_no = flds['cat'][0]

        play_data = {}
        play_data['play_info']  = play
        play_data['play_date']  = play_date
        play_data['play_start'] = play_start
        play_data['play_end']   = None
        play_data['play_dur']   = play_duration
        play_data['notes']      = None # ARRAY(Text)
        play_data['start_time'] = datetimetz(play_date, play_start, self.tz)
        play_data['end_time']   = play_data['start_time'] + play_duration if play_duration else None
        play_data['duration']   = play_duration

        play_data['ext_id']      = play.get('id')
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

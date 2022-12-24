# -*- coding: utf-8 -*-

"""ParserWFMT subclass implementation
"""

import datetime as dt
from typing import NewType, Any

import regex as re
from bs4 import BeautifulSoup, Tag

from .base import Parser, is_name_suffix, parse_rec_info
from ..core import log, LogicError, DataError
from ..utils import str2date, str2time, datetimetz
from ..playlist import Playlist
from ..musicent import ml_dict

#######################
# Constants/Functions #
#######################

DFLT_TIME_FMT = "%I:%M %p"

def parse_time_block(tb_str: str, time_fmt: str = DFLT_TIME_FMT) -> tuple[dt.time, dt.time]:
    """Format can be any of the following:
        '12:00 - 6:00 am'
        '10:00 am - 1:00 pm'
        '7:00 pm'
    """
    # parse out start and end times
    if ' - ' in tb_str:
        start, end = tb_str.split(' - ')
    else:
        start, end = tb_str, None

    # parse out hour:min from am/pm
    if ' ' in start:
        start_tm, start_ap = start.split(' ')
    else:
        start_tm, start_ap = start, None

    if end and ' ' in end:
        end_tm, end_ap = end.split(' ')
    else:
        end_tm, end_ap = end, None

    # return `(None, None)` for malformed input
    if not start:
        return None, None
    if not (start_ap or end_ap):
        return None, None

    if not start_ap:
        start += ' ' + end_ap
    start_time = str2time(start, time_fmt)
    end_time = str2time(end, time_fmt) if end else None

    return start_time, end_time

def get_play_variant(prog_data: dict) -> int | None:
    """TEMP/HACKY: for now, we do this based on program data, since
    we have it available, but this is not very neat--LATER, we will
    set this information in the parse "context" structure!!!

    A return value of `None` indicates the mainstream (non-variant)
    play format.
    """
    VARIANT_MAP = {
        'Through the Night with Peter van de Graaff': 1
    }
    return VARIANT_MAP.get(prog_data['name'])

def as_string(tag: Tag) -> str | None:
    """Convenience wrapper for extracting string from bs4 Tag element
    """
    if not tag:
        return None
    elif not tag.string:
        # note that this can be used for the simple string case as well,
        # but we separate it out to help identify the more complex tags
        # (e.g. "program-desc") for purposes of investigation/debugging
        return ' '.join(tag.stripped_strings)
    return tag.string.strip()

def as_stripped_strings(tag: Tag) -> list[str]:
    """Similar to `as_string()`, but used in cases where we want to treat
    individual strings (typically, separated by <br/> tags) discretely
    """
    return list(tag.stripped_strings) if tag else []

##############
# ParserWFMT #
##############

ProgPlay = NewType('ProgPlay', tuple[dt.date, Tag])
Play     = NewType('Play', Any)

class ParserWFMT(Parser):
    """Parser for WFMT station

    Note that the HTML for this station's playlists is "dense", in that there is no
    extraneous whitespace (newlines, etc.) for formatting.  This means we generally
    don't have to strip strings we are extracting from the bs4 tags.  This is a good
    thing--though, unfortunately, not everything is good (e.g. the complexity needed
    in map_plays()).
    """
    def iter_program_plays(self, playlist: Playlist) -> ProgPlay:
        """Iterator for program plays within a playlist, yield value is passed into
        map_program_play() and iter_plays())

        :param playlist: Playlist object
        :yield: subclass-specific program play representation
        """
        log.debug(f"Parsing json for {playlist.rel_path}")
        with open(playlist.file) as f:
            soup = BeautifulSoup(f, self.html_parser)

        wrap = soup.find('div', class_="entry-wrap")
        title_str = wrap.find('h6', id="schedule-title").string
        # "Playlist for Wednesday, October 19, 2022"
        if m := re.match(r'Playlist for \w+, (.+)', title_str):
            date_str = m.group(1)
            pl_date = str2date(date_str, "%B %d, %Y")
        else:
            # TODO: get date from file name (if needed)!!!
            raise DataError("Cannot get playlist date from title string")

        main = wrap.find('div', id="playlist-main")
        for prog_content in main.find_all('div', class_="content-block", recursive=False):
            yield pl_date, prog_content
        return

    def iter_plays(self, prog_play: ProgPlay) -> Play:
        """Iterator for plays within a program play, yield value is passed into map_play()

        :param prog_play: yield value from iter_program_plays()
        :yield: Play
        """
        pl_date, prog_content = prog_play

        for play_item in prog_content('div', class_="item", recursive=False):
            yield play_item
        return

    def map_program_play(self, prog_play: ProgPlay) -> dict:
        """
        :param prog_play: yield value from iter_program_plays()
        :return: dict of normalized program play information

        Notes:
          - `prog_play` looks like this:
              <div class="content-block">
                <h2 class="time-block">
                  6:00 - 10:00
                  <span style="font-size: smaller">
                    am
                  </span>
                </h2>
                <div class="program-info clearfix">
                  <p class="program-time">
                    6:00 am
                  </p>
                  <h3 class="program-title">
                    <a href="https://www.wfmt.com/programs/mornings-with-dennis-moore/" target="">
                      Mornings with Dennis Moore
                    </a>
                  </h3>
                  <p class="program-desc">
                    Including news &amp; weather on the hour between 6:00 am and 9:00 am; ...
                  </p>
                </div>
                <div class="item clearfix"></div>
                <div class="item clearfix"></div>
                <div class="item clearfix"></div>
                <div class="item clearfix"></div>
                        .
                        .
                        .
              </div>
        """
        pl_date, prog_content = prog_play

        # tags
        time_block = prog_content.find('h2', class_="time-block")
        info_block = prog_content.find('div', class_="program-info")

        # strings
        prog_time = as_string(info_block.find('p', class_="program-time"))
        prog_title = as_string(info_block.find('h3', class_="program-title"))
        # NOTE: `prog_desc` can represent the description for either the running program
        # or this particular episode--since we can't really tell which (without applying
        # a little NLP), we'll put it in the `notes` field for the prog_play (for now).
        # We can also do a little post-processing to populate `notes` for the program
        # itself if we see repeated entries for programs plays (i.e. episodes).
        prog_desc = as_string(info_block.find('p', class_="program-desc"))

        # consider `time_block` to be the authoritative represention of program play start
        # and end time (and not `prog_time`, see below)
        tb_str = ' '.join(as_stripped_strings(time_block))
        start_time, end_time = parse_time_block(tb_str)
        start_dt = datetimetz(pl_date, start_time, self.tz)
        end_dt = datetimetz(pl_date, end_time, self.tz) if end_time else None

        # if program-time exists, we'll do an integrity cross-check against time_block
        if prog_time:
            prog_start = str2time(prog_time, DFLT_TIME_FMT)
            if prog_start != start_time:
                log.debug(f"Time mismatch between \"{prog_time}\" and \"{tb_str}\"")

        # program data
        prog_data = {'name': prog_title, 'notes': None}

        # program play data
        pp_data = {}
        pp_data['prog_play_info']  = {'html': str(prog_content)}
        pp_data['prog_play_date']  = start_dt.date()
        pp_data['prog_play_start'] = start_dt.time()
        pp_data['prog_play_end']   = end_time
        pp_data['prog_play_dur']   = None  # Interval, if listed
        pp_data['notes']           = [prog_desc] if prog_desc else None
        pp_data['start_time']      = start_dt
        pp_data['end_time']        = end_dt
        pp_data['duration']        = end_dt - start_dt if end_dt else None

        pp_data['ext_id']          = None
        pp_data['ext_mstr_id']     = None

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
              <div class="item clearfix">
                <div class="time-played">
                  <p>
                    8:19 am
                  </p>
                </div>
                <div class="item-info">
                  <h4 class="composer-title">
                    <span class="composer">
                      Manuel Ponce
                    </span>
                    :
                    <span class="title">
                      "Concierto del Sur"
                    </span>
                  </h4>
                  <p class="orchestra-conductor">
                    Finale, Allegro moderato e festivo
                  </p>
                  <p class="soloists">
                    Pablo Sáinz Villegas, g; Phil Orch of the Americas/Alondra de la Parra
                    <br/>
                    Mi Alma Mexicana * My Mexican Soul
                  </p>
                  <p class="album-meta">
                    Sony 88697755552
                  </p>
                </div>
              </div>
        """
        prog_data = pp_norm['program']
        pp_data = pp_norm['program_play']

        # tags
        time_played = play.find('div', class_="time-played")
        item_info   = play.find('div', class_="item-info")
        comp_title  = item_info.find('h4', class_="composer-title")

        # strings
        comp       = as_string(comp_title.find('span', class_="composer"))
        title      = as_string(comp_title.find('span', class_="title"))
        orch_cond  = as_string(item_info.find('p', class_="orchestra-conductor"))
        album_meta = as_string(item_info.find('p', class_="album-meta"))
        soloists   = as_stripped_strings(item_info.find('p', class_="soloists"))

        play_date  = pp_data['prog_play_date']  # dt.date
        play_start = str2time(as_string(time_played), DFLT_TIME_FMT)
        play_end   = None  # dt.time
        play_dur   = None  # dt.timedelta

        comp_list = [comp]
        work_list = [title]
        perf_list = []
        cond_list = []
        ens_list  = []
        notes     = []
        rec_name  = None
        label     = None
        cat_no    = None

        match get_play_variant(prog_data):
            case None:
                # `orch_cond` (if present) misleadingly contains the name of the movement,
                # fragment, or some other form of the work's title, which we will save as
                # a "note" for the Play.  The actual orchestra/ensemble, with or without
                # the conductor, needs to be parsed from `soloists` (see next).
                #
                # It appears as if `soloists` can contain either one or two strings, the
                # first representing the performer(s), and the second (if present) being
                # the album title, but we'll handle the general case of perhaps more than
                # one separate performer strings (in addition to the album title)
                #
                # <perf_str>
                #   <perf_entry>; <perf_entry>; ...
                # <perf_entry>
                #   <orch_ens>/<cond>
                #   <orch_ens>/<cond>, <instr>
                #   <performer>, <instr>
                #   <performer> & <performer>, <instr>'s
                #   <orch_ens>
                if orch_cond:
                    notes.append(orch_cond)

                if len(soloists) > 1:
                    rec_name = soloists[-1]
                    if len(soloists) > 2:
                        log.debug(f"UNEXPECTED: multiple soloist(s) strings {soloists[:-1]}")
                for perf_str in soloists[:-1]:
                    for perf_entry in perf_str.split('; '):
                        if '/' in perf_entry:
                            orch_ens, cond_str = perf_entry.split('/')
                            # check to see if conductor has a second role (e.g. instrument)
                            cond_segs = cond_str.rsplit(', ', 1)
                            if len(cond_segs) > 1 and not is_name_suffix(cond_segs[1]):
                                cond, instr = cond_segs  # not stripped!!!
                                cond_list.append(cond)
                                perf_list.append(cond_str)
                            else:
                                cond_list.append(cond_str)
                            continue

                        if ', ' in perf_entry:
                            entry_segs = perf_entry.rsplit(', ', 1)
                            if len(entry_segs) > 1 and not is_name_suffix(entry_segs[1]):
                                soloist, instr = entry_segs  # not stripped!!!
                                perf_list.append(perf_entry)
                                # add individual entries for multi-performer listings
                                if ' & ' in soloist:
                                    # "p's" -> "p"
                                    if instr[-2:] == "'s":
                                        instr = instr[:-2]
                                    for indiv_perf in soloist.split(' & '):
                                        perf_list.append(f"{indiv_perf}, {instr}")
                                continue

                        # treat all other cases as an ensemble name
                        ens_list.append(perf_entry)
            case 1:
                # `orch_cond` examples:
                #   Swiss Italian Orch / Wolf-Dieter Hauschild
                #   London Symphony Orchestra / Gregor Bühl, conductor
                #   Royal Liverpool Phil
                #   Tokyo New Koto Ensemble
                #   Jerry Junkin
                #
                # `soloists` examples (not pretty):
                #   s,
                #   Charles Stier, clarinet,cl,
                #   András Schiff, piano,
                #   Kazue Kudo,koto,
                #   Gregory Allen,p,
                #   Havard Gimse,
                if orch_cond:
                    orch_segs = orch_cond.rsplit(' / ', 1)
                    if len(orch_segs) == 1:
                        # note that this could also mean a bare conductor name--LATER: we
                        # should decide whether to try and determine that here (using NER),
                        # or always do that processing downstream!!!
                        ens_list.append(orch_cond)
                    else:
                        orch, cond = orch_segs
                        if '/' in orch:
                            log.debug(f"UNEXPECTED: multiple slash in orch '{orch}'")
                        if cond[-11:] == ", conductor":
                            cond = cond[:-11]
                        ens_list.append(orch)
                        cond_list.append(cond)

                for solo_item in soloists:
                    solo_item = solo_item.rstrip(',')
                    solo_segs = solo_item.rsplit(', ', 1)
                    # try and identify this funny pattern for instrument: "piano,p" or
                    # "organ,hc"--here we assume that the instrument name/abbreviation
                    # bits are single words
                    if len(solo_segs) > 1 and re.fullmatch(r'[a-z,]', solo_segs[1]):
                        # if found, we leave it alone to be assigned to `instr` below
                        # (and hope that it is properly recognized downstream)
                        pass
                    else:
                        # otherwise, we ensure that any prior comma-delimiters have a
                        # trailing space, and we resplit
                        solo_segs = re.sub(r'\b,\b', ', ', solo_item).rsplit(', ', 1)
                    # now assume we are normalized (to the extent possible) and do the
                    # actual parsing/processing
                    if len(solo_segs) > 1 and not is_name_suffix(solo_segs[1]):
                        soloist, instr = solo_segs  # not stripped!!!
                        perf_list.append(solo_item)
                    elif re.fullmatch(r'[a-z]+', solo_item):
                        # bare all-lowercase string (e.g. "p", "v", "ms", "baryt", etc.)
                        # only indicates the instrument without the name of the soloist,
                        # so no useful information to record
                        pass
                    else:
                        # we only have the performer name (no instrument/role)
                        perf_list.append(solo_item)
            case variant:
                raise LogicError(f"Variant {variant} does not exist")

        # record info
        label, cat_no = parse_rec_info(album_meta)

        play_data = {}
        play_data['play_info']   = str(play)
        play_data['play_date']   = play_date
        play_data['play_start']  = play_start
        play_data['play_end']    = play_end
        play_data['play_dur']    = play_dur
        play_data['notes']       = notes or None
        play_data['start_time']  = datetimetz(play_date, play_start, self.tz)
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

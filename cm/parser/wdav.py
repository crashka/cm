# -*- coding: utf-8 -*-

"""ParserWDAV subclass implementation
"""

import regex as re
import datetime as dt
from typing import NewType

from bs4 import BeautifulSoup, Tag

from .base import Parser, parse_rec_info
from ..core import log
from ..utils import str2date, str2time, datetimetz
from ..playlist import Playlist
from ..musiclib import ml_dict

##############
# ParserWDAV #
##############

ProgPlay = NewType('ProgPlay', tuple[dt.date, Tag])
Play     = NewType('Play', tuple[Tag, Tag])

class ParserWDAV(Parser):
    """Parser for WDAV station

    Notes:
      - This station does not do a good job of inlining the program information,
        so we will skip it for now (probably not worth revisting even)
    """
    DFLT_PROG_NAME = "Classical Music"

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
        log.debug(f"Parsing html for {playlist.rel_path}")
        with open(playlist.file) as f:
            soup = BeautifulSoup(f, self.html_parser)

        title_str = soup.find('section', id="title").h4.string
        if m := re.match(r'WDAV Playlist for (.+)', title_str):
            date_str = m.group(1).strip()
            pl_date = str2date(date_str, "%b %d, %Y")
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
        pl_date, prog_wrapper = prog_play

        prog_name = self.DFLT_PROG_NAME
        prog_data = {'name': prog_name}

        start_dt = datetimetz(pl_date, dt.time(0, 0), self.tz)
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

    def map_play(self, pp_norm: dict, play: Play) -> tuple[ml_dict, dict]:
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
        :return: tuple of normalized play information and entity strings
        """
        pp_data = pp_norm['program_play']
        time_div, play_div = play
        play_date = pp_data['prog_play_date']
        play_time = str2time(time_div.string, "%I:%M %p")
        start_dt  = datetimetz(play_date, play_time, self.tz)

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

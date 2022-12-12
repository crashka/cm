# -*- coding: utf-8 -*-

"""ParserWDAV subclass implementation
"""

from os.path import relpath
import regex as re
import datetime as dt
from zoneinfo import ZoneInfo

from bs4 import BeautifulSoup, Tag

from .base import Parser
from ..core import log
from ..playlist import Playlist
from ..musiclib import ml_dict

##############
# ParserWDAV #
##############

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

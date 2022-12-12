# -*- coding: utf-8 -*-

"""ParserKUSC subclass implementation
"""

from os.path import relpath
import regex as re
import datetime as dt
from zoneinfo import ZoneInfo

from bs4 import BeautifulSoup

from .base import Parser, is_name_suffix
from ..core import log
from ..musiclib import ml_dict

##############
# ParserKUSC #
##############

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


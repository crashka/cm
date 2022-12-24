# -*- coding: utf-8 -*-

"""Base class implementation for the Parser module, also includes common declarations
and functions
"""

from typing import NewType, Any
from importlib import import_module
from zoneinfo import ZoneInfo

import regex as re

from ..utils import prettyprint
from ..core import env, log, DFLT_HTML_PARSER, ConfigError
from ..playlist import Playlist
from ..musicent import ml_dict, StringCtx, UNIDENT
from .. import musiclib as ml

###############################
# Commoon Constants/Functions #
###############################

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

def parse_rec_info(rec_info: str) -> tuple[str, str]:
    """Parse out label and catalog number from combined string, we split on whitespace
    and make interpretations on what the various segments mean based on how "catalogy-
    looking" they are

    Return tuple is (<label>, <cat_no>)
    """
    def is_catalogy(seg_str: str) -> bool:
        """Catalog number-looking string?
        """
        return bool(re.search(r'[0-9.-]+[0-9]$', seg_str))

    segs = re.split(r'\s+', rec_info)
    # some stations (e.g. WFMT) use a trailing "(<n>)" to indicate the number of
    # discs in the album; we don't care about this, so we'll just ignore it
    if re.fullmatch(r'\([0-9]+\)', segs[-1]):
        del segs[-1]

    if len(segs) == 1:
        return rec_info, None
    elif not is_catalogy(segs[-1]):
        return rec_info, None
    elif len(segs) == 2:
        # note that the logic below would actually handle this correctly, but
        # we'lll make it explicit here, since it is the most common case
        return tuple(segs)

    # otherwise we assume all trailing segments that look catalogy comprise the
    # catalog number (note, we already know the last segment qualifies, but we
    # include it in the iteration to keep the logic simpler)
    for i, seg in enumerate(segs[::-1]):
        if not is_catalogy(seg):
            return ' '.join(segs[:-i]), ' '.join(segs[-i:])

    # not quite sure what to do now, but we'll go with treating the last segment
    # as the catalog number, and the rest as the label name
    return ' '.join(segs[:-1]), ' '.join(segs[-1:])

#####################
# Parser base class #
#####################

ProgPlay = NewType('ProgPlay', Any)
Play     = NewType('Play', Any)

class Parser:
    """Helper class for Playlist, proper subclass is associated through station config
    """
    station:     'Station'
    tz:          ZoneInfo
    html_parser: str

    @classmethod
    def get_class(cls, sta: 'Station') -> type:
        """Return the parser subclass configured for the specified station
        """
        class_name = sta.parser_cls
        pkg_path = sta.parser_pkg
        if not class_name or not pkg_path:
            raise ConfigError(f"`parser_class` or `pkg_path` not specified for station '{sta.name}'")
        module = import_module('.', pkg_path)
        parser_class = getattr(module, class_name)
        if not issubclass(parser_class, cls):
            raise ConfigError(f"`{parser_class.__name__}` not subclass of `{cls.__name__}`")
        return parser_class

    def __new__(cls, sta: 'Station') -> 'Parser':
        parser_class = cls.get_class(sta)
        return super().__new__(parser_class)

    def __init__(self, sta: 'Station'):
        """Parser object is relatively stateless (just station backreference and parser
        directives), so constructor doesn't really do anything
        """
        self.station = sta
        self.tz = ZoneInfo(self.station.timezone)

        self.html_parser = env.get('html_parser') or DFLT_HTML_PARSER
        log.debug(f"HTML parser: {self.html_parser}")

    def proc_playlist(self, contents: str) -> str:
        """Process raw playlist contents downloaded from URL before saving to file.

        Note that base class implementation does generic processing, so should be called
        by subclass overrides.
        """
        # filter out NULL characters (lost track of which station this is needed for, so
        # we'll just do it generically for now)
        return contents.replace('\u0000', '')

    def iter_program_plays(self, playlist: Playlist) -> ProgPlay:
        """Iterator for program plays within a playlist, yield value is passed into
        map_program_play() and iter_plays())

        :param playlist: Playlist object
        :yield: subclass-specific program play representation
        """
        raise RuntimeError("abstract method iter_program_plays() must be subclassed")

    def iter_plays(self, prog_play: ProgPlay) -> Play:
        """Iterator for plays within a program play, yield value is passed into map_play()

        :param prog_play: yield value from iter_program_plays()
        :yield: subclass-specific play representation
        """
        raise RuntimeError("abstract method iter_plays() must be subclassed")

    def map_program_play(self, prog_play: ProgPlay) -> dict:
        """
        :param prog_play: yield value from iter_program_plays()
        :return: dict of normalized program play information
        """
        raise RuntimeError("abstract method map_program_play() must be subclassed")

    def map_play(self, pp_norm: dict, play: Play) -> tuple[ml_dict, dict]:
        """
        :param pp_norm: [dict] normalized program play information (from map_program_play())
        :param play: yield value from iter_plays()
        :return: tuple of normalized play information and entity strings
        """
        raise RuntimeError("abstract method map_play() must be subclassed")

    def parse(self, playlist: Playlist, dryrun: bool = False, force: bool = False) -> dict:
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
            pp_rec = ml.insert_program_play(playlist, pp_norm)
            if not pp_rec:
                raise RuntimeError("Could not insert program_play")
            playlist.parse_ctx['station_id']   = pp_rec['station_id']
            playlist.parse_ctx['prog_play_id'] = pp_rec['id']
            playlist.parse_ctx['play_id']      = None
            pp_rec['plays'] = []

            # Step 2 - Parse out play info (if present)
            for play in self.iter_plays(prog_play):
                play_norm, entity_str_data = self.map_play(pp_norm, play)
                if not play_norm:
                    continue
                # APOLOGY: perhaps this parsing of entity strings and merging into normalized
                # play data really belongs in the subclasses, but just hate to see all of the
                # exact replication of code--thus, we have this ugly, ill-defined interface,
                # oh well... (just need to be careful here)
                for composer_str in entity_str_data['composer']:
                    if composer_str:
                        play_norm.merge(ml.parse_composer_str(composer_str))
                for work_str in entity_str_data['work']:
                    if work_str:
                        play_norm.merge(ml.parse_work_str(work_str))
                for conductor_str in entity_str_data['conductor']:
                    if conductor_str:
                        play_norm.merge(ml.parse_conductor_str(conductor_str))
                for performers_str in entity_str_data['performers']:
                    if performers_str:
                        play_norm.merge(ml.parse_performer_str(performers_str))
                for ensembles_str in entity_str_data['ensembles']:
                    if ensembles_str:
                        play_norm.merge(ml.parse_ensemble_str(ensembles_str))

                play_rec = ml.insert_play(playlist, pp_rec, play_norm)
                if not play_rec:
                    raise RuntimeError("Could not insert play")
                playlist.parse_ctx['play_id'] = play_rec['id']
                pp_rec['plays'].append(play_rec)

                es_recs = ml.insert_entity_strings(playlist, entity_str_data)

                play_name = "%s - %s" % (play_norm['composer']['name'], play_norm['work']['name'])
                # TODO: create separate hash sequence for top of each hour!!!
                play_seq = playlist.hash_seq.add(play_name)
                if play_seq:
                    ps_recs = ml.insert_play_seq(play_rec, play_seq, 1)
                else:
                    log.debug("Skipping hash_seq for duplicate play:\n%s" % play_rec)

            # for prog_plays (after adding plays)...
            parsed_info.append(pp_rec)

        return parsed_info

    def analyze(self, playlist: Playlist, dryrun: bool = False, force: bool = False) -> None:
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
                play_norm, entity_str_data = self.map_play(pp_norm, play)
                if not play_norm:
                    continue
                # see APOLOGY in `parse()` above
                for composer_str in entity_str_data['composer']:
                    if composer_str:
                        play_norm.merge(ml.parse_composer_str(composer_str))
                for work_str in entity_str_data['work']:
                    if work_str:
                        play_norm.merge(ml.parse_work_str(work_str))
                for conductor_str in entity_str_data['conductor']:
                    if conductor_str:
                        play_norm.merge(ml.parse_conductor_str(conductor_str))
                for performers_str in entity_str_data['performers']:
                    if performers_str:
                        play_norm.merge(ml.parse_performer_str(performers_str))
                for ensembles_str in entity_str_data['ensembles']:
                    if ensembles_str:
                        play_norm.merge(ml.parse_ensemble_str(ensembles_str))

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

# -*- coding: utf-8 -*-

"""Music Library module
"""

import sys
import regex as re
from collections import UserDict
from collections.abc import Mapping, Iterable
import warnings

from sqlalchemy.exc import *

from .utils import LOV, str_similarity
from .core import env, log
from .database import DatabaseCtx

##############################
# common constants/functions #
##############################

# Lists of Values
NameVal = LOV({'NONE'   : '<none>',
               'UNKNOWN': '<unknown>'})

db = DatabaseCtx(env['database'])
ml_cache = {}

PK_WARNING = r'Column \'[\w\.]+\' is marked as a member of the primary key for table'

def get_entity(entity):
    if entity in ml_cache:
        return ml_cache[entity]
    handle = MusicEnt(entity)
    ml_cache[entity] = handle
    return handle

user_keys = {
    'person'        : {'name'},
    'performer'     : {'person_id', 'role'},
    'ensemble'      : {'name'},
    'work'          : {'composer_id', 'name'},
    'recording'     : {'label', 'catalog_no'},
    'recording_alt' : {'name', 'label'},
    'station'       : {'name'},
    'program'       : {'name', 'host_name'},
    'program_play'  : {'station_id', 'prog_play_date', 'prog_play_start', 'program_id'},
    'play'          : {'station_id', 'play_date', 'play_start', 'work_id'},
    'play_performer': {'play_id', 'performer_id'},
    'play_ensemble' : {'play_id', 'ensemble_id'},
    'play_seq'      : {'hash_level', 'hash_type', 'play_id'},
    'entity_string' : {'entity_str', 'source_fld', 'station_id'},
    'entity_ref'    : {'entity_ref', 'entity_type', 'ref_source'}
}

child_recs = {
    'person'        : [],
    'performer'     : ['person'],
    'ensemble'      : [],
    'work'          : [],
    'recording'     : [],
    'station'       : [],
    'program'       : [],
    'program_play'  : [],
    'play'          : [],
    'play_performer': [],
    'play_ensemble' : []
}

def clean_user_keys(data, entity):
    """Remove empty strings in user keys (set to None)

    :param data: dict of data elements
    :param entity: [string] name of entity
    """
    for k in user_keys[entity]:
        if data.get(k) == '':
            data[k] = None

def key_data(data, entity):
    """Return elements of entity data that are key fields

    :param data: dict of data elements
    :param entity: [string] name of entity
    :return: dict comprehension for key data elements
    """
    return {k: data[k] for k in set(data.keys()) & user_keys[entity]}

def entity_data(data, entity):
    """Return elements of entity data, excluding embedded child records (and later,
    other fields not belonging to the entity definition)

    :param data: dict of data elements
    :param entity: [string] name of entity
    :return: dict comprehension for entity data elements
    """
    return {k: v for k, v in data.items() if k not in child_recs[entity]}

class ml_dict(UserDict):
    """Manage data structure of this form:
    {
        'play'      : {},
        'composer'  : {},
        'work'      : {},
        'conductor' : {},
        'performers': [{}, ...],
        'ensembles' : [{}, ...],
        'recording' : {},
        'entity_str': {}
    }

    Future:
      - In particular, knows relationship between conductor, performers, and ensembles
        when merging
    """
    @staticmethod
    def deep_replace(d, from_str, to_str):
        """String replacement throughout structure
        """
        for k, v in d.items():
            if isinstance(v, Mapping):
                ml_dict.deep_replace(v, from_str, to_str)
            elif isinstance(v, Iterable):
                for m in v:
                    if isinstance(m, Mapping):
                        ml_dict.deep_replace(m, from_str, to_str)
            elif isinstance(v, str):
                d[k] = v.replace(from_str, to_str)

    def merge(self, to_merge):
        """Modifies current structure in place (no return value)

        :param to_merge: dict to merge from
        :return: void
        """
        for k, v in to_merge.items():
            if k not in self:
                self[k] = v
            elif isinstance(self[k], Iterable):
                if v:
                    self[k].extend(v)
                elif self[k]:  # i.e. non-empty list
                    log.trace("Skipping overwrite of ml_dict key \"%s\" (%s) with empty value" %
                              (k, str(self[k])))
            elif not self[k]:
                self[k] = v
            else:
                # LATER: do denormalizations into performers if needed, to ensure that
                # we don't lose data (for now, assumes that caller is already doing the
                # denorm--need to think about the proper model for this, either way)!!!
                log.debug("Not able to overwrite ml_dict key \"%s\" (%s) with (%s)" %
                          (k, str(self[k]), str(v)))

#################
# Parsing logic #
#################

COND_STRS = {'conductor',
             'cond.',
             'cond'}

# HACK: list of "magic" ensemble names to skip!!!
SKIP_ENS = {'ensemble',
            'soloists'}

SUFFIX_TOKEN = '{{SUFFIX}}'
UNIDENT      = '{{UNIDENT}}'

REPL_CHAR_16 = '\ufffd'
REPL_CHAR_8  = '\xef\xbf\xbd'

BRACKETS = {'"': '"',
            "'": "'",
            '(': ')',
            '[': ']'}

DELIMS = {}

NAME_RE   = r'[\p{L}\.,\' -]+'
ROLE_RE   = r'[\p{L}\.\'\(\)\/ -]+'
ROLE_RE2  = r'[\p{L}\.\'\(\)\/\, -]+'  # comma added for bracketed case
NAME_EXCL = r'[^\p{L}\.,\' -]'

ParseFlag = LOV({'COMPOSER' : 0x0001,
                 'CONDUCTOR': 0x0002,
                 'PERFORMER': 0x0004,
                 'ENSEMBLE' : 0x0008,
                 'WORK'     : 0x0010,
                 'LABEL'    : 0x0020,
                 'RECORDING': 0x0040,
                 # composite
                 'PERSON'   : 0x0007,
                 'TITLE'    : 0x0050})

class StringCtx(object):
    """
    """
    def __init__(self, ent_str, flags = 0):
        """
        """
        self.ent_str    = ent_str
        self.orig_str   = ent_str
        self.ctx_flags  = flags
        self.completion = []

    @staticmethod
    def examine_entity_str(ent_str, flags = 0):
        """Implement as stand-alone static method for now, possibly integrate with instance
        state and/or workflow later

        1. charset (unicode) fixups (e.g. replacement character)
        2. enclosing matched delimiters (quotes, parens, braces, etc.), entire string ("entity string")
        3. enclosing matched delimiters, substring ("entity item")
        4. incomplete matching delimiters (not terminated), for entire string (and items???)
        5. leading delimiters, for entire string and items
        6. item-separating delimiters
           6.a. identify and track position
           6.b. parse out individual fields
           6.c. classification (NER) of fields
           6.d. assemble entity items based on:
              6.d.i. delimiter hierarchy/significance
              6.d.ii logical field groupings
        """
        def get_entity_type(substr):
            """
            :param substr: item to lookup [string]
            :return: type [string]
            """
            er = get_entity('entity_ref')
            sel_res = er.select({'entity_ref': substr.strip()}, {'entity_strength': -1})
            if sel_res.rowcount > 0:
                er_row = sel_res.fetchone()
                return er_row.entity_type
            else:
                return None

        def examine_entity_fld(fid_str):
            """Examine a "field", which is what is between major delimiters; note that
            fields may contain one or more commas (typically not more than 2)

            :param: field [string]
            :return: pattern [string]
            """
            pass

        orig_str = ent_str
        log.debug("Examining entity string: \"%s\", flags: 0x%x" % (ent_str, flags))

        # Pre-Proc 1. charset/unicode and whitespace fixups
        repl8_count    = ent_str.count(REPL_CHAR_8)
        repl16_count   = ent_str.count(REPL_CHAR_16)
        dup_whitespace = bool(re.search(r'\s{2}', ent_str))
        trail_astrisks = bool(re.search(r'\*$', ent_str))
        if ent_str.count(REPL_CHAR_8):
            log.debug("  Fix utf-8 replacement char for \"%s\"" % ent_str)
            ent_str = ent_str.replace(REPL_CHAR_8, REPL_CHAR_16)
        if re.search(r'\s{2}', ent_str):
            log.debug("  Collapse whitespace for \"%s\"" % ent_str)
        if re.search(r'\*$', ent_str):
            log.debug("  Remove trailing astrisk(s) for \"%s\"" % ent_str)
            ent_str = ent_str.rstrip('*')

        # Pre-Proc 2. enclosing matched delimiters (quotes, parens, braces, etc.) for entire
        # entity string; ATTN: we currently only handle single character brackets!!!
        brackets = []
        while ent_str and  ent_str[0] in BRACKETS.keys():
            open_char  = ent_str[0]
            cls_char   = BRACKETS[open_char]
            if open_char == cls_char:
                count_char = ent_str.count(open_char)
                count_open = count_char // 2 + count_char % 2
                count_cls  = count_char // 2
                is_matched = count_open == count_cls
                is_encl    = ent_str[-1] == cls_char
            else:
                count_char = None
                count_open = ent_str.count(open_char)
                count_cls  = ent_str.count(cls_char)
                is_matched = count_open == count_cls
                is_encl    = ent_str[-1] == cls_char

            brackets.append({'open_char' : open_char,
                             'cls_char'  : cls_char,
                             'count_char': count_char,
                             'count_open': count_open,
                             'count_cls' : count_cls,
                             'is_matched': is_matched,
                             'is_encl'   : is_encl})

            if is_encl:
                # always remove outer bracket chars
                log.debug("  Remove enclosing bracket chars for \"%s\"" % ent_str)
                ent_str = ent_str[1:-1]
            elif count_open - count_cls == 1:
                # strip off leading bracket char
                log.debug("  Strip leading bracket char for \"%s\"" % ent_str)
                ent_str = ent_str[1:]
            else:
                if not is_matched:
                    log.debug("  EES_WARN - mismatched interior bracket char(s) for \"%s\"" % ent_str)
                break

        if ent_str != orig_str:
            log.debug("             cleaned-up: \"%s\"" % ent_str)
        ent_ptrn1 = None
        ent_ptrn2 = None
        ent_ptrn3 = None

        # pass 1 - split using major delimiters '/', ';', ' - ', ' * ' (mandatory spaces as
        # indicated, otherwise optional space both before and after)
        DELIMS_PTRN = r'( ?\/ ?| ?\; ?| \- | \* | \& )'
        ent_list  = []  # [(ent_fld, ent_start, delim_str, delim_end), ...]
        ent_elems = []
        unidents  = []
        ent_start = 0
        # add extra entry to pick up trailing entity
        delim_matches = list(re.finditer(DELIMS_PTRN, ent_str)) + [None]
        log.debug("  Pass 1 - delim matches: %d" % (len(delim_matches)))
        for m in delim_matches:
            if m:
                ent_end   = m.start()  # a.k.a. delim_start
                delim_end = m.end()
                delim_str = m.group()
            else:
                ent_end   = None  # represents end of ent_str
                delim_end = None
                delim_str = ''
            # note, leading delimiter yields ent_end == 0 and empty ent_fld
            assert not ent_end or ent_start < ent_end
            ent_fld = ent_str[ent_start:ent_end]
            assert ent_fld == ent_fld.strip()
            ent_list.append((ent_fld, ent_start, delim_str, delim_end))
            log.debug("    Entity item %s" % (str(ent_list[-1])))
            if ent_fld:
                ent_type = get_entity_type(ent_fld)
                if ent_type:
                    ent_elems.append("{{%s}}" % ent_type + delim_str)
                    log.debug("      Appending ent_elem \"%s\"" % str(ent_elems[-1]))
                else:
                    unidents.append((ent_fld, ent_start, delim_str, delim_end))
                    ent_elems.append(UNIDENT + delim_str)
                    log.debug("      Appending unident %s" % str(unidents[-1]))
                    log.debug("      Appending ent_elem \"%s\"" % str(ent_elems[-1]))
            else:
                ent_elems.append(delim_str)
                log.debug("      Appending ent_elem \"%s\"" % str(ent_elems[-1]))
            ent_start = delim_end

        ent_ptrn1 = ''.join(ent_elems)
        log.debug("    Entity pattern 1 \"%s\"" % ent_ptrn1)

        # pass 2 - find entities among/across comma-deliminted expressions
        if ent_str.count(',') > 0:
            DELIMS_PTRN = r'( ?, ?)'
            ents1 = []  # [(ent_fld, ent_start, delim_str, delim_end), ...]
            ents2 = []
            ents3 = []
            ent_matches = []
            unidents  = []
            ent_start = 0
            # add extra entry to pick up trailing entity
            delim_matches = list(re.finditer(DELIMS_PTRN, ent_str)) + [None]
            log.debug("  Pass 2 - delim matches: %d" % (len(delim_matches)))
            for m in delim_matches:
                if m:
                    ent_end   = m.start()  # a.k.a. delim_start
                    delim_end = m.end()
                    delim_str = m.group()
                else:
                    ent_end   = 99999  # represents end of ent_str
                    delim_end = 99999
                    delim_str = ''
                # note, leading delimiter yields ent_end == 0 and empty ent_fld
                assert not ent_end or ent_start < ent_end
                ent_fld = ent_str[ent_start:ent_end]
                assert ent_fld == ent_fld.strip()

                ents1.append((ent_fld, ent_start, delim_str, delim_end))
                log.debug("    Entity item1 %s" % (str(ents1[-1])))
                if len(ents1) > 1:
                    ent_fld2 = ents1[-2][0] + ents1[-2][2] + ents1[-1][0]
                    ents2.append((ent_fld2, ents1[-2][1], delim_str, delim_end))
                    log.debug("    Entity item2 %s" % (str(ents2[-1])))
                    if len(ents1) > 2:
                        ent_fld3 = ents1[-3][0] + ents1[-3][2] + ents2[-1][0]
                        ents3.append((ent_fld3, ents1[-3][1], delim_str, delim_end))
                        log.debug("    Entity item3 %s" % (str(ents3[-1])))
                ent_start = delim_end

            # build list of matches
            for ent_item in ents3 + ents2 + ents1:
                ent_fld   = ent_item[0]
                ent_start = ent_item[1]
                delim_str = ent_item[2]
                delim_end = ent_item[3]
                if ent_fld:
                    ent_type = get_entity_type(ent_fld)
                    if ent_type:
                        ent_matches.append((ent_item, "{{%s}}" % ent_type + delim_str))
                        log.debug("    Entity match %s" % (str(ent_matches[-1])))
                    else:
                        unidents.append((ent_item, UNIDENT + delim_str))
                        log.debug("    Entity match %s" % (str(unidents[-1])))
                else:
                    ent_matches.append((ent_item, delim_str))
                    log.debug("    Entity match %s" % (str(ent_matches[-1])))

            # one more pass to find best [sic] fit (just do brainless N x M iteration for now)
            ptrn_elems = []  # [(ent_item, ent_substr), ...]
            for ent_item, ent_substr in ent_matches + unidents:
                conflict = False
                for ptrn_item, ptrn_substr in ptrn_elems:
                    if not (ent_item[3] <= ptrn_item[1] or ent_item[1] >= ptrn_item[3]):
                        conflict = True
                        break
                if not conflict:
                    ptrn_elems.append((ent_item, ent_substr))
                    log.debug("    Pattern elem %s" % (str(ptrn_elems[-1])))
            # sort by position, validate no gaps (TODO: also validate against delim_matches!!!)
            ptrn_elems.sort(key=lambda elem: elem[0][1])
            prev_end = 0
            for elem in ptrn_elems:
                elem_start = elem[0][1]
                elem_end = elem[0][3]
                assert elem_start == prev_end
                prev_end = elem_end

            ent_ptrn2 = ''.join([elem[1] for elem in ptrn_elems])
            log.debug("    Entity pattern 2 \"%s\"" % ent_ptrn2)

            # TODO: look for bracketed/quoted entities within unidents; try and reconstruct
            # pattern based on associations (e.g. instrument/role -> performer)!!!

        return ent_ptrn1, ent_ptrn2, ent_ptrn3

    def parse_entity_str(self, flags = 0):
        """
        1. charset (unicode) fixups (e.g. replacement character)
        2. enclosing matched delimiters (quotes, parens, braces, etc.), entire string ("entity string")
        3. enclosing matched delimiters, substring ("entity item")
        4. incomplete matching delimiters (not terminated), for entire string (and items???)
        5. leading delimiters, for entire string and items
        6. item-separating delimiters
           6.a. identify and track position
           6.b. parse out individual fields
           6.c. classification (NER) of fields
           6.d. assemble entity items based on:
              6.d.i. delimiter hierarchy/significance
              6.d.ii logical field groupings
        """
        # REVISIT: or work directly on self.ent_str???
        ent_str = self.ent_str
        flags |= self.ctx_flags

        # Rule 1. charset/unicode and whitespace fixups
        if ent_str.count(REPL_CHAR_8):
            log.debug("PES_RULE 1a - fix utf-8 replacement char for \"%s\"" % ent_str)
            ent_str = ent_str.replace(REPL_CHAR_8, REPL_CHAR_16)
        if re.search(r'\s{2}', ent_str):
            log.debug("PES_RULE 1b - collapsing whitespace for \"%s\"" % ent_str)
            ent_str = re.sub(r'\s{2,}', ' ', ent_str)

        # Rule 2. enclosing matched delimiters (quotes, parens, braces, etc.), entire string
        # ("entity string"); ATTN: we currently only handle single character brackets!!!
        while ent_str and  ent_str[0] in BRACKETS.keys():
            open_char  = ent_str[0]
            cls_char   = BRACKETS[open_char]
            if open_char == cls_char:
                count_char = ent_str.count(open_char)
                count_open = count_char // 2 + count_char % 2
                count_cls  = count_char // 2
                is_matched = count_open == count_cls
                is_encl    = ent_str[-1] == cls_char
            else:
                count_open = ent_str.count(open_char)
                count_cls  = ent_str.count(cls_char)
                is_matched = count_open == count_cls
                is_encl    = ent_str[-1] == cls_char

            if is_encl:
                # always remove outer bracket chars
                log.debug("PES_RULE 2a - remove enclosing bracket chars for \"%s\"" % ent_str)
                ent_str = ent_str[1:-1]
            elif count_open - count_cls == 1:
                # strip off leading bracket char
                log.debug("PES_RULE 2b - strip leading bracket char for \"%s\"" % ent_str)
                ent_str = ent_str[1:]
            else:
                if not is_matched:
                    log.debug("PES_WARN - mismatched interior bracket char(s) for \"%s\"" % ent_str)
                break

        # Rule 3. enclosing matched delimiters, substring ("entity item")
        if flags & ParseFlag.TITLE:
            title_str = self.parse_title_str(ent_str)
            # LATER: make this conditional based on some sanity checks!!!
            ent_str = title_str

        # Rule 4. incomplete matching delimiters (not terminated), for entire string (and items???)

        # Rule 5. leading delimiters, for entire string and items

        # Rule 6. preserve suffixes introduced by commas (e.g. "Jr.", "Sr.", etc.)
        # (factor out from regular comma/delimiter processing)
        if flags & ParseFlag.PERSON:
            person_str = self.parse_person_str(ent_str)
            # LATER: make this conditional based on some sanity checks!!!
            ent_str = person_str

        # Rule 7. item-separating delimiters

        """
           7.a. identify and track position
           7.b. parse out individual fields
           7.c. classification (NER) of fields
           7.d. assemble entity items based on:
              7.d.i. delimiter hierarchy/significance
              7.d.ii logical field groupings
        """

        self.ent_str = ent_str
        return self.ent_str

    def parse_person_str(self, person_str, flags = 0):
        """
        """
        orig_str = person_str
        flags |= self.ctx_flags

        # step 2 - preserve suffixes introduced by commas (e.g. "Jr.", "Sr.", etc.) (factor out
        # from regular comma processing)
        m = re.search(r'(,? (?:Jr|Sr)\.?)(?:\W|$)', person_str, flags=re.I)
        if m:
            suffix = m.group(1)
            log.debug("PPS_RULE 6 - preserve suffix \"%s\" for \"%s\"" % (suffix, person_str))
            person_str = person_str.replace(suffix, SUFFIX_TOKEN, 1)

            def restore_sfx(ent_data):
                """
                :param ent_data:
                :return: void (or should this return a new data structure???)
                """
                ml_dict.deep_replace(ent_data, SUFFIX_TOKEN, suffix.title())
            self.completion.append(restore_sfx)

        # step 3 - fix "Last, First" (handle "Last, First Middle ..."); note, we are also
        # coelescing spaces (might as well)
        m = re.fullmatch(r'([\w\ufffd<>-]+),((?:\s+[\w\ufffd-]+)+)', person_str)
        if m:
            log.debug("PPS_RULE 4 - reverse \"Last, First [...]\" for \"%s\"" % person_str)
            person_str = "%s %s" % (re.sub(r'\s{2,}', ' ', m.group(2).lstrip()), m.group(1))

        # step 4 - handle non-comma-introduced suffixes (e.g. "II") and compound last names (e.g.
        # Vaughan Williams)

        # step 5 - multiple names (e.g. "/" or "&" or "and" or ",")

        # step 6 - "arr.", "arranged", "orch.", "orchestrated", etc. (for composer)

        # step 7 - remove conductor role suffix ("cond.", "conductor", etc.)
        if flags & ParseFlag.CONDUCTOR:
            m = re.fullmatch(r'(.+), ([\w\./ ]+)', person_str)
            if m:
                if m.group(2).lower() in COND_STRS:
                    log.debug("PPS_RULE 5 - removing role suffix \"%s\" for \"%s\"" %
                              (m.group(2), person_str))
                    person_str = m.group(1)

        return person_str

    def parse_title_str(self, title_str, flags = 0):
        """
        """
        orig_str = title_str
        flags |= self.ctx_flags

        # step 3 - convert single-quoted titles to double-quoted
        # note, we are currently biased toward better-formed quotes toward end of title
        # LATER: try with different biases, and determine best-formed result!!!)
        m = re.fullmatch(r'(.*)\'([^\']*)\'([^\']*)', title_str)
        while m:
            log.debug("PTS_RULE 3 - convert single-quoted titles to double quotes \"%s\"" % title_str)
            title_str = "%s\"%s\"%s" % (m.group(1), m.group(2), m.group(3))
            m = re.fullmatch(r'(.*)\'([^\']*)\'([^\']*)', title_str)

        return title_str

    def finalize(self, ent_data, flags = 0):
        """
        :param ent_data: ml_dict to finalize (in-place)
        :param flags: int
        :return: void
        """
        flags |= self.ctx_flags

        for func in self.completion:
            func(ent_data)

    def mkcomp(self, name, orig_str = None):
        orig_str = orig_str or self.orig_str
        name = name.strip()
        if not name:
            log.notice("Empty composer name \"%s\", parsed from \"%s\"" %
                       (name, orig_str))
        elif not re.match(r'\w', name):
            log.notice("Bad leading character in composer \"%s\", parsed from \"%s\"" %
                       (name, orig_str))
        return {'name': name, 'raw_name': orig_str if name != orig_str else None, 'is_composer': True}

    def mkwork(self, name, orig_str = None):
        orig_str = orig_str or self.orig_str
        name = name.strip()
        if not name:
            log.notice("Empty work name \"%s\", parsed from \"%s\"" %
                       (name, orig_str))
        elif not re.match(r'\w', name):
            log.notice("Bad leading character in work \"%s\", parsed from \"%s\"" %
                       (name, orig_str))
        return {'name': name, 'raw_name': orig_str if name != orig_str else None}

    def mkcond(self, name, orig_str = None):
        orig_str = orig_str or self.orig_str
        name = name.strip()
        if not name:
            log.notice("Empty conductor name \"%s\", parsed from \"%s\"" %
                       (name, orig_str))
        elif not re.match(r'\w', name):
            log.notice("Bad leading character in conductor \"%s\", parsed from \"%s\"" %
                       (name, orig_str))
        return {'name': name, 'raw_name': orig_str if name != orig_str else None, 'is_conductor': True}

    def mkperf(self, name, role, orig_str = None):
        orig_str = orig_str or self.orig_str
        name = name.strip()
        if role:
            role = role.strip()
        if not name:
            log.notice("Empty performer name \"%s\" [%s], parsed from \"%s\"" %
                       (name, role, orig_str))
        elif not re.match(r'\w', name):
            log.notice("Bad leading character in performer \"%s\" [%s], parsed from \"%s\"" %
                       (name, role, orig_str))
        perf_person = {'name': name, 'raw_name': orig_str if name != orig_str else None}
        if role not in COND_STRS:
            perf_person['is_performer'] = True
        return {'person': perf_person, 'role': role}

    def mkens(self, name, orig_str = None):
        orig_str = orig_str or self.orig_str
        name = name.strip()
        if not name:
            log.notice("Empty ensemble name \"%s\", parsed from \"%s\"" %
                       (name, orig_str))
        elif not re.match(r'\w', name):
            log.notice("Bad leading character in ensemble \"%s\", parsed from \"%s\"" %
                       (name, orig_str))
        return {'name': name, 'raw_name': orig_str if name != orig_str else None}

###############################
# String/entity normalization #
###############################

NormFlag = LOV({'INCL_SELF' : 0x0001})

HONORIFICS = {'Frei', 'Sir', 'Count', 'Comtessa', 'Compte',
              'Sister', 'Dame', 'Capt.', 'Cpl.', 'Rev.', 'Dr.'}
SUFFIXES   = {'Jr.', 'Sr.', 'Jr', 'Sr', 'II', 'III', 'IV'}
CODEX_LIST = {'Codex', 'Tablature', 'Manuscript', 'Book', 'Breviary',
              'Hymnorum', 'Cordiforme', 'Nonnberg', 'Ottelio'}
ANONYMOUS  = {'Anonymous', 'Unknown'}

def normalize_name(name, flags = 0):
    """Normalize a western-style name

    ATTENTION: currently expecting input to be "Last, First", though this handles variations
    on placement of honorifics and suffixes, as well as some common malformed inputs

    :param name:
    :param flags:
    :return: tuple of (normalized_name [str], aliases [set of str], raw_name [str])
    """
    orig_name  = name
    normalized = None
    aliases    = set()
    honor      = None
    suffix     = None
    suffix_sep = None
    codex      = None
    anon       = None

    # collapse/fix whitespace and punctuation, if needed
    if re.search(r'\s{2}', name):
        name = re.sub(r'\s{2,}', ' ', name)
    if re.search(r',{2}', name):
        name = re.sub(r',{2,}', ',', name)
    if re.search(r',\S', name):
        name = re.sub(r',(\S)', r', \1', name)
    name = name.strip(' ,;')

    parts = name.split(', ')
    # don't try and do too much here (i.e. single-comma case as well), instead handle
    # notices below (at the risk of unstreamlining)
    if len(parts) > 2:
        parts_set = set(parts)
        hnr = parts_set & HONORIFICS
        sfx = parts_set & SUFFIXES
        cdx = parts_set & CODEX_LIST
        ano = parts_set & ANONYMOUS

        if hnr:
            honor = hnr.pop()
            parts.remove(honor)
            aliases.add(', '.join(parts))
            if hnr:
                log.notice("Don't know how to handle multiple honorifics in \"%s\"" % name)
        if sfx:
            suffix = sfx.pop()
            suffix_sep = ', '
            parts.remove(suffix)
            # PONDER: should we add this???
            #aliases.add(' '.join(parts))
            if sfx:
                log.notice("Don't know how to handle multiple suffixes in \"%s\"" % name)
        if cdx:
            codex = cdx.pop()
            parts.remove(codex)
            if hnr:
                log.notice("Don't know how to handle multiple codexes in \"%s\"" % name)
        if ano:
            anon = ano.pop()
            parts.remove(anon)
            if ano:
                log.notice("Don't know how to handle multiple anons in \"%s\"" % name)
    if not suffix:
        if parts[-1] in SUFFIXES:
            # special case for malformed input (e.g. "First Last, Jr.", where listing by
            # last name is expected)
            suffix = parts.pop(-1)
            suffix_sep = ', '
        else:
            # note, this pattern is overly-generic for the separator (given parsing above),
            # but leave this way, since it conveys the larger intent
            pattern = r'(.+)(,? )(%s)' % ('|'.join({s.replace('.', r'\.') for s in SUFFIXES}))
            m = re.fullmatch(pattern, parts[-1])
            if m:
                parts[-1]  = m.group(1)
                suffix_sep = m.group(2)
                suffix     = m.group(3)
    if parts[0] in ANONYMOUS:
        assert not anon
        # NOTE: just do this here (rather than "anon = parts.pop(0)", etc.), to keep things
        # simple (relatively speaking)
        if len(parts) > 1:
            parts[0] += ','
            aliases.add(' '.join(parts[1:]))
    else:
        # REVISIT: this is kind of brash, should really validate this is what we want
        # in all cases!!!
        parts.reverse()
        if parts[0] in ANONYMOUS:
            assert not anon
            # see NOTE, above (also applies here)
            if len(parts) > 1:
                parts[0] += ','
                aliases.add(' '.join(parts[1:]))
    # TODO: check for logical inconsistencies (soft mutual exlusion)!!!
    if honor:
        parts.insert(0, honor)
    if suffix:
        parts[-1] += suffix_sep + suffix
    if codex:
        parts.insert(0, codex)
    if anon:
        parts.insert(0, anon + ',')
    normalized = ' '.join(parts)

    # build aliases for nicknames (capture delimiter to distinguish matching pattern)
    pattern1 = r'(%s) (\")(%s)\" (%s)' % (NAME_RE, NAME_RE, NAME_RE)
    pattern2 = r'(%s) (\()(%s)\) (%s)' % (NAME_RE, NAME_RE, NAME_RE)
    m = re.fullmatch(pattern1, normalized) or re.fullmatch(pattern2, normalized)
    if m:
        aliases.add("%s %s" % (m.group(1), m.group(4)))
        aliases.add("%s %s" % (m.group(3), m.group(4)))
        # add a quoted version for either case (don't worry about a paren'ed version)
        aliases.add("\"%s\" %s" % (m.group(3), m.group(4)))
        if m.group(2) == '(':
            aliases.add("%s \"%s\" %s" % (m.group(1), m.group(3), m.group(4)))
    elif re.search(NAME_EXCL, normalized):
        log.notice("Non-standard char(s) in normalized name \"%s\" (raw: \"%s\")" %
                   (normalized, name))

    if not honor:
        pattern = r"(%s) (.+)" % ('|'.join(HONORIFICS))
        m = re.fullmatch(pattern, normalized)
        if m:
            honor = m.group(1)
            aliases.add(m.group(2))
    while len(parts) > 2:
        parts.pop(0)
        aliases.add(' '.join(parts))
    # REVISIT: not sure we really want to do this (especially if/when we know it comes in
    # malfored)--perhaps better left to caller's discretion (modulo slight fixup, above)!!!
    if normalized != name and flags & NormFlag.INCL_SELF:
        aliases.add(name)

    return normalized, aliases, name

##################
# MusicEnt class #
##################

class MusicEnt(object):
    def __init__(self, entity):
        """
        :param entity: [string] name of entity (same as table name)
        """
        self.name     = entity
        self.tab      = db.get_table(self.name)
        self.cols     = {c.name for c in self.tab.columns}
        self.last_sel = None
        self.last_ins = None
        self.last_upd = None
        self.last_del = None

    def select(self, crit, order_by = None):
        """
        :param crit: dict of query criteria
        :param order_by: list of column names, or dict of names mapped to +/-1 (asc/desc)
        :return: SQLAlchemy ResultProxy
        """
        if not crit:
            raise RuntimeError("Query criteria must be specified")
        unknown = set(crit) - self.cols
        if unknown:
            raise RuntimeError("Unknown column(s) for \"%s\": %s" % (self.name, str(unknown)))

        sel = self.tab.select()
        for col, val in crit.items():
            # NOTE: there was previously a problem (exception) with a timedelta (Interval) field in
            # the query crit, not sure why--we don't currently need to special-case remove that from
            # crit any more, but the error may come up again in the future, so may need investigate
            # again if/when it does
            sel = sel.where(self.tab.c[col] == val)
        if order_by:
            if isinstance(order_by, str):
                order_by = {order_by: 1}
            elif isinstance(order_by, Iterable):
                order_by = {c: 1 for c in order_by}
            for col, dir_ in order_by.items():
                sel = sel.order_by(self.tab.c[col] if dir_ >= 0 else self.tab.c[col].desc())
        with db.conn.begin() as trans:
            res = db.conn.execute(sel)
        self.last_sel = sel
        return res

    def insert(self, data):
        """
        :param data: dict of data to insert
        :return: SQLAlchemy ResultProxy
        """
        if not data:
            raise RuntimeError("Insert data must be specified")
        unknown = set(data) - self.cols
        if unknown:
            raise RuntimeError("Unknown column(s) for \"%s\": %s" % (self.name, str(unknown)))

        with warnings.catch_warnings():
            warnings.filterwarnings('ignore', message=PK_WARNING)
            ins = self.tab.insert()
            with db.conn.begin() as trans:
                res = db.conn.execute(ins, data)
        self.last_ins = ins
        return res

    def update(self, row, data):
        """
        :param row: SQLAlchemy RowProxy from select statement
        :param data: dict of data to update
        :return: SQLAlchemy ResultProxy
        """
        if not data:
            raise RuntimeError("Update data must be specified")
        unknown = set(data) - self.cols
        if unknown:
            raise RuntimeError("Unknown column(s) for \"%s\": %s" % (self.name, str(unknown)))

        upd = self.tab.update()
        for col, val in key_data(row, self.name).items():
            upd = upd.where(self.tab.c[col] == val)
        with db.conn.begin() as trans:
            res = db.conn.execute(upd, data)
        # TODO: update row._row with updated data values!!!
        self.last_upd = upd
        return res

    def inserted_row(self, res, ent_override = None):
        """
        :param res: SQLAlchemy ResultProxy from insert statement
        :param ent_override: e.g. used if _alt entity
        :return: SQLAlchemy RowProxy if exactly one row returned, otherwise None
        """
        params = res.last_inserted_params()

        if ent_override:
            sel_res = self.select(key_data(params, ent_override))
        else:
            sel_res = self.select(key_data(params, self.name))

        return sel_res.fetchone() if sel_res.rowcount == 1 else None

    def inserted_primary_key(self, res, ent_override = None):
        """res.inserted_primary_key is not currently working (probably due to the use_identity()
        hack), so need to requery new row to get the primary key

        :param res: SQLAlchemy ResultProxy from insert statement
        :param ent_override: e.g. used if _alt entity
        :return: primary key of inserted row (or None, if row not [uniquely] identified)
        """
        ins_row = self.inserted_row(res, ent_override)
        return ins_row.id if ins_row else None

##################
# MusicLib class #
##################

class MusicLib(object):
    """Helper class for writing playlist data into the database
    """
    def __init__(self):
        """
        """
        pass

    def insert_program_play(self, playlist, data):
        """
        :param playlist: parent Playlist object
        :param data: normalized playlist key/value data (dict)
        :return: key-value dict comprehension for inserted program_play fields
        """
        station = playlist.station
        # TODO: get rid of this hardwired structure (implicitly use fields from config file)!!!
        sta_data = {'name'      : station.name,
                    'timezone'  : station.timezone,
                    'synd_level': station.synd_level}
        sta = get_entity('station')
        sel_res = sta.select(key_data(sta_data, 'station'))
        if sel_res.rowcount == 1:
            sta_row = sel_res.fetchone()
        else:
            log.trace("Inserting station \"%s\" into musiclib" % station.name)
            ins_res = sta.insert(sta_data)
            if ins_res.rowcount == 0:
                raise RuntimeError("Could not insert station \"%s\" into musiclib" % station.name)
            sta_row = sta.inserted_row(ins_res)
            if not sta_row:
                raise RuntimeError("Station %s not in musiclib" % station.name)

        prog_data = data['program']
        prog = get_entity('program')
        sel_res = prog.select(key_data(prog_data, 'program'))
        if sel_res.rowcount == 1:
            prog_row = sel_res.fetchone()
        else:
            prog_name = prog_data['name']  # for convenience
            prog_label = "\"%s\"" % (prog_name)
            log.trace("Inserting program %s into musiclib" % prog_label)
            ins_res = prog.insert(prog_data)
            if ins_res.rowcount == 0:
                raise RuntimeError("Could not insert program %s into musiclib" % prog_label)
            prog_row = prog.inserted_row(ins_res)
            if not prog_row:
                raise RuntimeError("Program %s not in musiclib" % prog_label)

        pp_row = None
        pp_data = data['program_play']
        pp_data['station_id'] = sta_row.id
        pp_data['program_id'] = prog_row.id
        prog_play = get_entity('program_play')
        try:
            ins_res = prog_play.insert(pp_data)
            pp_row = prog_play.inserted_row(ins_res)
            log.trace("Created program_play ID %d (%s, \"%s\", %s %s)" %
                      (pp_row.id, sta_row.name, prog_row.name,
                       pp_row.prog_play_date, pp_row.prog_play_start))
        except IntegrityError:
            # TODO: need to indicate duplicate to caller (currenty looks like an insert)!!!
            sel_res = prog_play.select(key_data(pp_data, 'program_play'))
            if sel_res.rowcount == 1:
                pp_row = sel_res.fetchone()
                log.debug("Skipping insert of duplicate program_play record (ID %d)" % pp_row.id)
            else:
                pass  # REVISIT: is this an internal error???
        return {k: v for k, v in pp_row.items()} if pp_row else None

    def insert_play(self, playlist, prog_play, data):
        """
        :param playlist: parent Playlist object
        :param prog_play: parent program_play fields (dict)
        :param data: normalized play key/value data (dict)
        :return: key-value dict comprehension for inserted play fields
        """
        station = playlist.station
        # TODO: see above, insert_program_play()!!!  In addition, note that we should
        # really not have to requery here--the right thing to do is cache the station
        # record (in fact, we really only need the id here)!!!
        sta_data = {'name'      : station.name,
                    'timezone'  : station.timezone,
                    'synd_level': station.synd_level}
        sta = get_entity('station')
        sel_res = sta.select(key_data(sta_data, 'station'))
        if sel_res.rowcount == 1:
            sta_row = sel_res.fetchone()
        else:
            # NOTE: should really never get here (select should not fail), since this
            # same code was executed when inserting the program_play
            log.trace("Inserting station \"%s\" into musiclib" % station.name)
            ins_res = sta.insert(sta_data)
            if ins_res.rowcount == 0:
                raise RuntimeError("Could not insert station \"%s\" into musiclib" % station.name)
            sta_row = sta.inserted_row(ins_res)
            if not sta_row:
                raise RuntimeError("Station %s not in musiclib" % station.name)

        comp_data = data['composer']
        # NOTE: we always make sure there is a composer record (even if NONE or UNKNOWN), since work depends
        # on it (and there is no play without work, haha)
        if not comp_data.get('name'):
            comp_data['name'] = NameVal.NONE
        comp = get_entity('person')
        sel_res = comp.select(key_data(comp_data, 'person'))
        if sel_res.rowcount == 1:
            comp_row = sel_res.fetchone()
            if not comp_row.is_composer:
                comp.update(comp_row, {'is_composer': True})
        else:
            comp_name = comp_data['name']  # for convenience
            log.trace("Inserting composer \"%s\" into musiclib" % comp_name)
            ins_res = comp.insert(comp_data)
            if ins_res.rowcount == 0:
                raise RuntimeError("Could not insert composer/person \"%s\" into musiclib" % comp_name)
            comp_row = comp.inserted_row(ins_res)
            if not comp_row:
                raise RuntimeError("Composer/person \"%s\" not in musiclib" % comp_name)

        work_data = data['work']
        if not work_data.get('name'):
            # REVISIT: for how, insert '<unknown>' work, just so we have a record of this and can
            # try and identify the scenario (note, should really be logging to exception table)!!!
            work_data['name'] = NameVal.UNKNOWN
            #log.debug("Work name not specified, skipping...")
            #return None
        work_data['composer_id'] = comp_row.id
        work = get_entity('work')
        sel_res = work.select(key_data(work_data, 'work'))
        if sel_res.rowcount == 1:
            work_row = sel_res.fetchone()
        else:
            work_name = work_data['name']  # for convenience
            log.trace("Inserting work \"%s\" into musiclib" % work_name)
            ins_res = work.insert(work_data)
            if ins_res.rowcount == 0:
                raise RuntimeError("Could not insert work/person \"%s\" into musiclib" % work_name)
            work_row = work.inserted_row(ins_res)
            if not work_row:
                raise RuntimeError("Work/person \"%s\" not in musiclib" % work_name)

        cond_row = None
        cond_data = data['conductor']
        if cond_data.get('name'):
            cond = get_entity('person')
            sel_res = cond.select(key_data(cond_data, 'person'))
            if sel_res.rowcount == 1:
                cond_row = sel_res.fetchone()
                if not cond_row.is_conductor:
                    cond.update(cond_row, {'is_conductor': True})
            else:
                cond_name = cond_data['name']  # for convenience
                log.trace("Inserting conductor \"%s\" into musiclib" % cond_name)
                ins_res = cond.insert(cond_data)
                if ins_res.rowcount == 0:
                    raise RuntimeError("Could not insert conductor/person \"%s\" into musiclib" % cond_name)
                cond_row = cond.inserted_row(ins_res)
                if not cond_row:
                    raise RuntimeError("Conductor/person \"%s\" not in musiclib" % cond_name)

        rec_row = None
        rec_data = data['recording']
        clean_user_keys(rec_data, 'recording')
        clean_user_keys(rec_data, 'recording_alt')
        if rec_data.get('label') and rec_data.get('catalog_no'):
            rec = get_entity('recording')
            sel_res = rec.select(key_data(rec_data, 'recording'))
            if sel_res.rowcount == 1:
                rec_row = sel_res.fetchone()
            else:
                rec_ident = "%s %s" % (rec_data['label'], rec_data['catalog_no'])  # for convenience
                log.trace("Inserting recording \"%s\" into musiclib" % rec_ident)
                ins_res = rec.insert(rec_data)
                if ins_res.rowcount == 0:
                    raise RuntimeError("Could not insert recording \"%s\" into musiclib" % rec_ident)
                rec_row = rec.inserted_row(ins_res)
                if not rec_row:
                    raise RuntimeError("Recording \"%s\" not in musiclib" % rec_ident)
        elif rec_data.get('name'):
            rec = get_entity('recording')
            sel_res = rec.select(key_data(rec_data, 'recording_alt'))
            if sel_res.rowcount == 1:
                rec_row = sel_res.fetchone()
            elif sel_res.rowcount > 1:
                # REVISIT: just pick the first one randomly???
                rec_row = sel_res.fetchone()
            else:
                rec_name = rec_data['name']  # for convenience
                log.trace("Inserting recording \"%s\" into musiclib" % rec_name)
                ins_res = rec.insert(rec_data)
                if ins_res.rowcount == 0:
                    raise RuntimeError("Could not insert recording \"%s\" into musiclib" % rec_name)
                rec_row = rec.inserted_row(ins_res, 'recording_alt')
                if not rec_row:
                    raise RuntimeError("Recording \"%s\" not in musiclib" % rec_name)

        perf_rows = []
        for perf_data in data['performers']:
            # STEP 1 -: insert/select underlying person record
            perf_person = get_entity('person')  # cached, so okay to re-get for each loop
            sel_res = perf_person.select(key_data(perf_data['person'], 'person'))
            if sel_res.rowcount == 1:
                perf_person_row = sel_res.fetchone()
                if perf_data['role'] not in COND_STRS and not perf_person_row.is_performer:
                    perf_person.update(perf_person_row, {'is_performer': True})
            else:
                perf_name = perf_data['person']['name']  # for convenience
                log.trace("Inserting performer/person \"%s\" into musiclib" % perf_name)
                ins_res = perf_person.insert(perf_data['person'])
                if ins_res.rowcount == 0:
                    raise RuntimeError("Could not insert performer/person \"%s\" into musiclib" % perf_name)
                perf_person_row = perf_person.inserted_row(ins_res)
                if not perf_person_row:
                    raise RuntimeError("Performer/person \"%s\" not in musiclib" % perf_name)
            perf_data['person_id'] = perf_person_row.id

            # STEP 2 - now deal with performer record (since we have the person)
            perf = get_entity('performer')  # cached, so okay to re-get for each loop
            sel_res = perf.select(key_data(perf_data, 'performer'))
            if sel_res.rowcount == 1:
                perf_row = sel_res.fetchone()
            else:
                perf_name = perf_data['person']['name']  # for convenience
                perf_role = perf_data['role']
                perf_label = "\"%s\" [%s]" % (perf_name, perf_role)
                log.trace("Inserting performer %s into musiclib" % perf_label)
                ins_res = perf.insert(entity_data(perf_data, 'performer'))
                if ins_res.rowcount == 0:
                    raise RuntimeError("Could not insert performer %s into musiclib" % perf_label)
                perf_row = perf.inserted_row(ins_res)
                if not perf_row:
                    raise RuntimeError("Performer %s not in musiclib" % perf_label)
            perf_rows.append(perf_row)

        ens_rows = []
        for ens_data in data['ensembles']:
            ens = get_entity('ensemble')  # cached, so okay to re-get for each loop
            sel_res = ens.select(key_data(ens_data, 'ensemble'))
            if sel_res.rowcount == 1:
                ens_row = sel_res.fetchone()
            else:
                ens_name = ens_data['name']  # for convenience
                log.trace("Inserting ensemble \"%s\" into musiclib" % ens_name)
                ins_res = ens.insert(ens_data)
                if ins_res.rowcount == 0:
                    raise RuntimeError("Could not insert ensemble \"%s\" into musiclib" % ens_name)
                ens_row = ens.inserted_row(ins_res)
                if not ens_row:
                    raise RuntimeError("Ensemble \"%s\" not in musiclib" % ens_name)
            ens_rows.append(ens_row)

        play_new = False
        play_row = None
        play_data = data['play']
        play_data['station_id']   = sta_row.id
        play_data['prog_play_id'] = prog_play['id']
        play_data['program_id']   = prog_play['program_id']
        play_data['composer_id']  = comp_row.id
        play_data['work_id']      = work_row.id
        if cond_row:
            play_data['conductor_id'] = cond_row.id
        # NOTE: performer_ids and ensemble_ids are denorms, with no integrity checking
        if perf_rows:
            play_data['performer_ids'] = [perf_row.id for perf_row in perf_rows]
        if ens_rows:
            play_data['ensemble_ids'] = [ens_row.id for ens_row in ens_rows]
        play = get_entity('play')
        try:
            ins_res = play.insert(play_data)
            play_row = play.inserted_row(ins_res)
            play_new = True
            log.trace("Created play ID %d (%s, \"%s\", %s %s)" %
                      (play_row.id, comp_row.name, work_row.name,
                       play_row.play_date, play_row.play_start))
        except IntegrityError:
            # TODO: need to indicate duplicate to caller (currenty looks like an insert)!!!
            log.debug("Skipping insert of duplicate play record:\n%s" % play_data)
            sel_res = play.select(key_data(play_data, 'play'))
            if sel_res.rowcount == 1:
                play_row = sel_res.fetchone()
            else:
                pass  # REVISIT: is this an internal error???

        # write intersect records that are authoritative (denormed as arrays of keys, above)
        play_perf_rows = []
        play_ens_rows = []
        if play_new:
            for perf_row in perf_rows:
                play_perf_data = {'play_id': play_row.id, 'performer_id': perf_row.id}
                play_perf = get_entity('play_performer')
                try:
                    ins_res = play_perf.insert(play_perf_data)
                    play_perf_rows.append(play_perf.inserted_row(ins_res))
                except IntegrityError:
                    log.trace("Skipping insert of duplicate play_performer record:\n%s" % play_perf_data)

            for ens_row in ens_rows:
                play_ens_data = {'play_id': play_row.id, 'ensemble_id': ens_row.id}
                play_ens = get_entity('play_ensemble')
                try:
                    ins_res = play_ens.insert(play_ens_data)
                    play_ens_rows.append(play_ens.inserted_row(ins_res))
                except IntegrityError:
                    log.trace("Skipping insert of duplicate play_ensemble record:\n%s" % play_ens_data)

        return {k: v for k, v in play_row.items()}

    def insert_play_seq(self, play_rec, play_seq, hash_type):
        """
        :param play_rec:
        :param play_seq:
        :param hash_type:
        :return: list of key-value dict comprehensions for inserted play_seq fields
        """
        ret = []
        ps = get_entity('play_seq')
        while play_seq:
            level = len(play_seq)
            hashval = play_seq.pop(0)
            data = {
                'hash_level': level,
                'hash_type' : hash_type,
                'play_id'   : play_rec['id'],
                'seq_hash'  : hashval
            }

            try:
                ins_res = ps.insert(data)
                ps_row = ps.inserted_row(ins_res)
                ret.append({k: v for k, v in ps_row.items()})
            except IntegrityError:
                log.debug("Could not insert play_seq %s into musiclib" % data)

        return ret

    def insert_entity_strings(self, playlist, data):
        """
        :return: list of key-value dict comprehensions for inserted entity_string fields
        """
        ctx = playlist.parse_ctx
        ret = []
        es = get_entity('entity_string')
        for entity_src, src_strings in data.items():
            for entity_str in src_strings:
                if not (entity_str and re.search(r'\w', entity_str)):
                    continue
                ent_str_data = {
                    'entity_str'  : entity_str,
                    'source_fld'  : entity_src,
                    'station_id'  : ctx['station_id'],
                    'prog_play_id': ctx['prog_play_id'],
                    'play_id'     : ctx['play_id']
                }

                try:
                    ins_res = es.insert(ent_str_data)
                    es_row = es.inserted_row(ins_res)
                    ret.append({k: v for k, v in es_row.items()})
                except IntegrityError:
                    log.trace("Duplicate entity_string \"%s\" [%s] for station ID %d" %
                              (entity_str, entity_src, ctx['station_id']))

        return ret

    def insert_entity_ref(self, refdata, ent_data, ent_refs, raw_name = None):
        """
        :return: key-value dict comprehension for inserted entity_ref fields
        """
        #ctx = refdata.parse_ctx
        ent_name = ent_data['entity_ref']
        ent_type = ent_data['entity_type']
        ref_source = ent_data['ref_source']
        ret = []
        er = get_entity('entity_ref')

        try:
            ins_res = er.insert(ent_data)
            es_row = er.inserted_row(ins_res)
            ret.append({k: v for k, v in es_row.items()})
        except IntegrityError:
            log.trace("Duplicate entity name \"%s\" [%s] for refdata \"%s\"" %
                      (ent_name, ent_type, ref_source))

        ent_ref_data = ent_data.copy()
        del ent_ref_data['is_entity']
        for ref_str in ent_refs:
            ent_ref_data['entity_ref'] = ref_str,
            ent_ref_data['ref_strength'] = round(str_similarity(ref_str, ent_name) * 100.0)
            # FIX: see comment in caller (refdata.py)!!!
            if raw_name and ref_str == raw_name:
                ent_ref_data['is_raw'] = True
            elif ent_ref_data.get('is_raw'):
                del ent_ref_data['is_raw']

            try:
                ins_res = er.insert(ent_ref_data)
                es_row = er.inserted_row(ins_res)
                ret.append({k: v for k, v in es_row.items()})
            except IntegrityError:
                log.trace("Duplicate entity_ref \"%s\" [%s] for refdata \"%s\"" %
                          (ref_str, ent_type, ref_source))

        return ret

    def parse_composer_str(self, comp_str, flags = 0):
        """
        :param comp_str: raw string from playlist
        :param flags: [int/bitfield] later
        :return: ml_dict of parsed data
        """
        if not comp_str or not re.search(r'\w', comp_str):
            return {}

        ctx = StringCtx(comp_str, flags | ParseFlag.COMPOSER)
        ctx.parse_entity_str()

        comp_data = ctx.mkcomp(ctx.ent_str)
        ent_data = ml_dict({'composer': comp_data})
        ctx.finalize(ent_data)
        return ent_data

    def parse_work_str(self, work_str, flags = 0):
        """
        :param work_str: raw string from playlist
        :param flags: [int/bitfield] later
        :return: ml_dict of parsed data
        """
        if not work_str or not re.search(r'\w', work_str):
            return {}

        ctx = StringCtx(work_str, flags | ParseFlag.WORK)
        ctx.parse_entity_str()

        work_data = ctx.mkwork(ctx.ent_str)
        ent_data = ml_dict({'work': work_data})
        ctx.finalize(ent_data)
        return ent_data

    def parse_conductor_str(self, cond_str, flags = 0):
        """
        :param cond_str: raw string from playlist
        :param flags: [int/bitfield] later
        :return: ml_dict of parsed data
        """
        if not cond_str or not re.search(r'\w', cond_str):
            return {}

        ctx = StringCtx(cond_str, flags | ParseFlag.CONDUCTOR)
        ctx.parse_entity_str()

        cond_data = ctx.mkcond(ctx.ent_str)
        perf_data = [ctx.mkperf(ctx.ent_str, 'conductor')]
        ent_data = ml_dict({'conductor': cond_data, 'performers': perf_data})
        ctx.finalize(ent_data)
        return ent_data

    def parse_performer_str(self, perf_str, flags = 0):
        """
        DESIGN NOTES (for future):
          * context-sensitive application of individual parsing rules, either implicitly
            (e.g. based on station), or explicitly through flags
          * generic parsing using non-alphanum delimiters, entity lookups (refdata), and
            logical entity relationships (either as replacement, or complement)
          * for now, we return performer data only; LATER: need the ability to indicate
            other entities extracted from perf_str!!!

        :param perf_str:
        :param flags: (not yet implemented)
        :return: list of perf_data structures (see LATER above)
        """
        if not perf_str or not re.search(r'\w', perf_str):
            return {}

        ctx = StringCtx(perf_str, flags | ParseFlag.PERFORMER)
        ctx.parse_entity_str()

        def parse_perf_item(perf_item, fld_delim = ','):
            sub_perfs = []
            sub_cond = None
            if perf_item.count(fld_delim) % 2 == 1:
                fields = perf_item.split(fld_delim)
                while fields:
                    pers, role = (fields.pop(0), fields.pop(0))
                    # special case for "<ens>/<cond last, first>"
                    if pers.count('/') == 1:
                        log.debug("PFS_RULE 6 - slash separating ens from cond_last \"%s\"" % pers)
                        ens_name, cond_last = pers.split('/')
                        cond_name = "%s %s" % (role, cond_last)
                        sub_perfs.append(ctx.mkperf(ens_name, 'ensemble'))
                        sub_perfs.append(ctx.mkperf(cond_name, 'conductor'))
                    else:
                        if role.strip().lower() in COND_STRS:
                            # TODO: check for overwrite!!!
                            sub_cond = ctx.mkcond(pers)
                        sub_perfs.append(ctx.mkperf(pers, role))
            else:
                # TODO: if even number of field delimiters, need to look closer at item
                # contents/format to figure out what to do!!!
                sub_perfs.append(ctx.mkperf(perf_item, None))
            return {'performers': sub_perfs, 'conductor': sub_cond} if sub_cond \
                   else {'performers': sub_perfs}

        ens_data  = []
        perf_data = []
        ret_data  = ml_dict({'ensembles': ens_data, 'performers': perf_data})
        """
        # TODO: should really move the quote processing as far upstream as possible (for
        # all fields); NOTE: also need to revisit normalize_* functions in musiclib!!!
        m = re.fullmatch(r'"([^"]*)"', perf_str)
        if m:
            log.debug("PFS_RULE 1 - strip enclosing quotes \"%s\"" % (perf_str))
            perf_str = m.group(1)  # note: could be empty string, handle downstream!
        m = re.fullmatch(r'\((.*[^)])\)?', perf_str)
        if m:
            log.debug("PFS_RULE 2 - strip enclosing parens \"%s\"" % (perf_str))
            perf_str = m.group(1)  # note: could be empty string, handle downstream!
        """
        # TODO: genericize performer/person/role stuff (note, ctx.ent_str not updated below)!!!
        perf_str = ctx.ent_str

        # special case for ugly record (WNED 2018-09-17)
        m = re.match(r'(.+?)\r', perf_str)
        if m:
            log.debug("PFS_RULE 3 - ugly broken record for WNED \"%s\"" % perf_str)
            perf_str = m.group(1)
            m = re.match(r'(.+)\[(.+)\],(.+)', perf_str)
            if m:
                perf_str = '; '.join(m.groups())

        # pattern used by IPR, VPR, WIAA, WNED
        if re.match(r'\/.+ \- ', perf_str):
            log.debug("PFS_RULE 4 - leading slash for performer fields \"%s\"" % perf_str)
            for perf_item in perf_str.split('/'):
                if perf_item:
                    ret_data.merge(parse_perf_item(perf_item, ' - '))
        elif ';' in perf_str:
            log.debug("PFS_RULE 5 - semi-colon-deliminted performer fields \"%s\"" % perf_str)
            for perf_item in perf_str.split(';'):
                if perf_item:
                    ret_data.merge(parse_perf_item(perf_item))
        elif perf_str:
            ret_data.merge(parse_perf_item(perf_str))

        ctx.finalize(ret_data)
        return ret_data

    def parse_ensemble_str(self, ens_str, flags = 0):
        """
        :param ens_str:
        :param flags: (not yet implemented)
        :return: dict of ens_data/perf_data structures, indexed by type
        """
        if not ens_str or not re.search(r'\w', ens_str):
            return {}

        ctx = StringCtx(ens_str, flags | ParseFlag.ENSEMBLE)
        ctx.parse_entity_str()

        def parse_ens_item(ens_item, fld_delim = ','):
            sub_ens_data = []
            sub_perf_data = []
            if ens_item.count(fld_delim) % 2 == 1:
                fields = ens_item.split(fld_delim)
                while fields:
                    name, role = (fields.pop(0), fields.pop(0))
                    # TEMP: if role starts with a capital letter, assume the whole string
                    # is an ensemble (though in reality, it may be two--we'll deal with
                    # that later, when we have NER), otherwise treat as performer/role!!!
                    #if re.match(r'[A-Z]', role[0]):
                    if re.match(r'\p{Lu}', role[0]):
                        sub_ens_data.append(ctx.mkens(name))
                    else:
                        sub_perf_data.append(ctx.mkperf(name, role))
            else:
                # TODO: if even number of field delimiters, need to look closer at item
                # contents/format to figure out what to do (i.e. NER)!!!
                sub_ens_data.append(ctx.mkens(ens_item))
            return {'ensembles' : sub_ens_data, 'performers': sub_perf_data}

        def parse_ens_fields(fields):
            sub_ens_data = []
            sub_perf_data = []
            while fields:
                if len(fields) == 1:
                    sub_ens_data.append(ctx.mkens(fields.pop(0)))
                    break  # same as continue
                # more reliable to do this moving backward from the end (sez me)
                if ' ' not in fields[-1]:
                    # REVISIT: we presume a single-word field to be a city/location (for now);
                    # as above, we should really look at field contents to properly parse!!!
                    ens = ','.join([fields.pop(-2), fields.pop(-1)])
                    sub_ens_data.append(ctx.mkens(ens))
                else:
                    # yes, do this twice!
                    sub_ens_data.append(ctx.mkens(fields.pop(-1)))
                    sub_ens_data.append(ctx.mkens(fields.pop(-1)))
            return {'ensembles' : sub_ens_data, 'performers': sub_perf_data}

        ens_data  = []
        perf_data = []
        ret_data  = ml_dict({'ensembles': ens_data, 'performers': perf_data})
        if ';' in ens_str:
            for ens_item in ens_str.split(';'):
                if ens_item:
                    ret_data.merge(parse_ens_item(ens_item))
        elif ',' in ens_str:
            ens_fields = ens_str.split(',')
            ret_data.merge(parse_ens_fields(ens_fields))
        else:
            # ens_data is implcitly part of ret_data
            ens_data.append(ctx.mkens(ens_str))

        return ret_data

#####################
# command line tool #
#####################

if __name__ == '__main__':
    if len(sys.argv) < 4:
        raise RuntimeError("Usage: musiclib.py [select|insert] <entity> <key>=<value> ...")

    ent = get_entity(sys.argv[2])
    meth = getattr(ent, sys.argv[1])
    data = {}
    for cond in sys.argv[3:]:
        (key, val) = cond.split('=')
        data[key] = int(val) if val.isdigit() else val
    res = meth(data)
    print("Rowcount: %d" % res.rowcount)
    if res.returns_rows:
        print(res.fetchall())
    else:
        print(res.__dict__)

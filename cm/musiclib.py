# -*- coding: utf-8 -*-

"""Music Library module
"""

from __future__ import absolute_import, division, print_function

import sys
import regex as re
import datetime as dt

from sqlalchemy import bindparam
from sqlalchemy.sql import func
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.types import DateTime
from sqlalchemy.exc import *

from core import cfg, env, log, dbg_hand
from database import DatabaseCtx
from utils import LOV, prettyprint, collecttype

##############################
# common constants/functions #
##############################

# Lists of Values
NameVal = LOV({'NONE'   : '<none>',
               'UNKNOWN': '<unknown>'})

db = DatabaseCtx(env['database'])
ml_cache = {}

def get_entity(entity):
    if entity in ml_cache:
        return ml_cache[entity]
    handle = MusicEnt(entity)
    ml_cache[entity] = handle
    return handle

user_keys = {
    'person'        : ['name'],
    'performer'     : ['person_id', 'role'],
    'ensemble'      : ['name'],
    'work'          : ['composer_id', 'name'],
    'recording'     : ['label', 'catalog_no'],
    'recording_alt' : ['name', 'label'],
    'station'       : ['name'],
    'program'       : ['name', 'host_name'],
    'program_play'  : ['station_id', 'prog_play_date', 'prog_play_start', 'program_id'],
    'play'          : ['station_id', 'play_date', 'play_start', 'work_id'],
    'play_performer': ['play_id', 'performer_id'],
    'play_ensemble' : ['play_id', 'ensemble_id'],
    'play_seq'      : ['hash_level', 'hash_type', 'play_id'],
    'entity_string' : ['entity_str', 'source_fld', 'station_id']
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
    return {k: data[k] for k in data.viewkeys() & user_keys[entity]}

def entity_data(data, entity):
    """Return elements of entity data, excluding embedded child records (and later,
    other fields not belonging to the entity definition)

    :param data: dict of data elements
    :param entity: [string] name of entity
    :return: dict comprehension for entity data elements
    """
    return {k: v for k, v in data.items() if k not in child_recs[entity]}

class ml_dict(dict):
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
      - subclass from UserDict (python3)
    """
    def merge(self, to_merge):
        """Modifies current structure in place (no return value)

        :param to_merge: dict to merge from
        :return: void
        """
        for k, v in to_merge.items():
            if k not in self:
                self[k] = v
            elif collecttype(self[k]):
                if v:
                    self[k].extend(v)
                elif self[k]:  # i.e. non-empty list
                    log.debug("Skipping overwrite of ml_dict key \"%s\" (%s) with empty value" %
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

COND_STRS = set(['conductor',
                 'cond.',
                 'cond'])

# HACK: list of "magic" ensemble names to skip!!!
SKIP_ENS = set(['ensemble',
                'soloists'])

SUFFIX_TOKEN = '<<SUFFIX>>'

def mkcomp(name, orig_str):
    name = name.strip()
    if not name:
        log.warn("Empty composer name \"%s\", parsed from \"%s\"" %
                 (name, orig_str))
    elif not re.match(r'\w', name):
        log.warn("Bad leading character in composer \"%s\", parsed from \"%s\"" %
                 (name, orig_str))
    return {'name'    : name,
            'raw_name': orig_str if name != orig_str else None}

def mkwork(name, orig_str):
    name = name.strip()
    if not name:
        log.warn("Empty work name \"%s\", parsed from \"%s\"" %
                 (name, orig_str))
    elif not re.match(r'\w', name):
        log.warn("Bad leading character in work \"%s\", parsed from \"%s\"" %
                 (name, orig_str))
    return {'name'    : name,
            'raw_name': orig_str if name != orig_str else None}

def mkcond(name, orig_str):
    name = name.strip()
    if not name:
        log.warn("Empty conductor name \"%s\", parsed from \"%s\"" %
                 (name, orig_str))
    elif not re.match(r'\w', name):
        log.warn("Bad leading character in conductor \"%s\", parsed from \"%s\"" %
                 (name, orig_str))
    return {'name'    : name,
            'raw_name': orig_str if name != orig_str else None}

def mkperf(name, role, orig_str):
    name = name.strip()
    if role:
        role = role.strip()
    if not name:
        log.warn("Empty performer name \"%s\" [%s], parsed from \"%s\"" %
                 (name, role, orig_str))
    elif not re.match(r'\w', name):
        log.warn("Bad leading character in performer \"%s\" [%s], parsed from \"%s\"" %
                 (name, role, orig_str))
    return {'person': {'name'    : name,
                       'raw_name': orig_str if name != orig_str else None},
            'role'  : role}

def mkens(name, orig_str):
    name = name.strip()
    if not name:
        log.warn("Empty ensemble name \"%s\", parsed from \"%s\"" %
                 (name, orig_str))
    elif not re.match(r'\w', name):
        log.warn("Bad leading character in ensemble \"%s\", parsed from \"%s\"" %
                 (name, orig_str))
    return {'name'    : name,
            'raw_name': orig_str if name != orig_str else None}

def parse_composer_str(comp_str, flags = None):
    """Modifies data in-place

    :param comp_str: raw string from playlist
    :param flags: [int/bitfield] later
    :return: ml_dict of parsed data
    """
    if not comp_str:
        return {}

    orig_comp_str = comp_str

    # step 1 - overall line fixup--TODO: see note in parse_performer_str()!!!
    m = re.match(r'"([^"]*)"$', comp_str)
    if m:
        log.debug("PCS_RULE 1 - strip enclosing quotes \"%s\"" % (comp_str))
        comp_str = m.group(1)  # note: could be empty string, handle downstream!
    m = re.match(r'\((.*[^)])\)?$', comp_str)
    if m:
        log.debug("PCS_RULE 2 - strip enclosing parens \"%s\"" % (comp_str))
        comp_str = m.group(1)  # note: could be empty string, handle downstream!

    # step 2 - preserve suffixes introduced by commas (e.g. "Jr.", "Sr.", etc.) (factor out
    # from regular comma processing)
    suffix = None
    m = re.search(r'(,\s+(?:Jr|Sr)\.?)', comp_str)
    if m:
        suffix = m.group(1)
        log.debug("PCS_RULE 3 - preserve suffix \"%s\" for \"%s\"" % (suffix, comp_str))
        comp_str = comp_str.replace(suffix, SUFFIX_TOKEN, 1)

    # step 3 - fix "Last, First" (handle "Last, First Middle ..."); note, we are also
    # coelescing spaces (might as well)
    m = re.match(r'([\w<>-]+),((?:\s+[\w-]+)+)$', comp_str)
    if m:
        log.debug("PCS_RULE 4 - reverse \"Last, First [...]\" for \"%s\"" % (comp_str))
        comp_str = "%s %s" % (re.sub(r'\s{2,}', ' ', m.group(2).lstrip()), m.group(1))

    # step 4 - handle non-comma-introduced suffixes (e.g. "II") and compound last names (e.g.
    # Vaughan Williams)

    # step 5 - multiple names (e.g. "/" or "&" or "and" or ",")

    # step 6 - "arr.", "arranged", "orch.", "orchestrated", etc. (for composer)

    # step N - finally...
    if suffix:
        comp_str = comp_str.replace(SUFFIX_TOKEN, suffix, 1)

    comp_data = mkcomp(comp_str, orig_comp_str)
    return ml_dict({'composer': comp_data})

def parse_work_str(work_str, flags = None):
    """Modifies data in-place

    :param work_str: raw string from playlist
    :param flags: [int/bitfield] later
    :return: ml_dict of parsed data
    """
    if not work_str:
        return {}

    orig_work_str = work_str

    # step 1 - overall line fixup--TODO: see note in parse_performer_str()!!!
    m = re.match(r'"(.*)"$', work_str)
    if m:
        log.debug("PWS_RULE 1a - strip enclosing double quotes \"%s\"" % (work_str))
        work_str = m.group(1)  # note: could be empty string, handle downstream!

    # step 2 - fix up doubled-up double quotes
    m = re.match(r'(.*?)""([^"]*)""(.*)$', work_str)
    while m:
        log.debug("PWS_RULE 2 - fix up doubled-up double quotes '%s'" % (work_str))
        work_str = "%s\"%s\"%s" % (m.group(1), m.group(2), m.group(3))
        m = re.match(r'(.*?)""([^"]*)""(.*)$', work_str)

    # step 3 - convert single-quoted titles to double-quoted
    m = re.match(r'(.*?)\'([^\']*)\'(.*)$', work_str)
    while m:
        log.debug("PWS_RULE 3 - convert single-quoted titles to double quotes '%s'" % (work_str))
        work_str = "%s\"%s\"%s" % (m.group(1), m.group(2), m.group(3))
        m = re.match(r'(.*?)\'([^\']*)\'(.*)$', work_str)

    work_data = mkwork(work_str, orig_work_str)
    return ml_dict({'work': work_data})

def parse_conductor_str(cond_str, flags = None):
    """Modifies data in-place

    :param cond_str: raw string from playlist
    :param flags: [int/bitfield] later
    :return: ml_dict of parsed data
    """
    if not cond_str:
        return {}

    orig_cond_str = cond_str

    # step 1 - overall line fixup--TODO: see note in parse_performer_str()!!!
    m = re.match(r'"([^"]*)"$', cond_str)
    if m:
        log.debug("PDS_RULE 1 - strip enclosing quotes \"%s\"" % (cond_str))
        cond_str = m.group(1)  # note: could be empty string, handle downstream!
    m = re.match(r'\((.*[^)])\)?$', cond_str)
    if m:
        log.debug("PDS_RULE 2 - strip enclosing parens \"%s\"" % (cond_str))
        cond_str = m.group(1)  # note: could be empty string, handle downstream!

    # step 2 - preserve suffixes introduced by commas (e.g. "Jr.", "Sr.", etc.) (factor out
    # from regular comma processing)
    suffix = None
    m = re.search(r'(,\s+(?:Jr|Sr)\.?)', cond_str)
    if m:
        suffix = m.group(1)
        log.debug("PDS_RULE 3 - preserve suffix \"%s\" for \"%s\"" % (suffix, cond_str))
        cond_str = cond_str.replace(suffix, SUFFIX_TOKEN, 1)

    # step 3 - fix "Last, First" (handle "Last, First Middle ..."); note, we are also
    # coelescing spaces (might as well)
    m = re.match(r'([\w<>-]+),((?:\s+[\w-]+)+)$', cond_str)
    if m:
        log.debug("PDS_RULE 4 - reverse \"Last, First [...]\" for \"%s\"" % (cond_str))
        cond_str = "%s %s" % (re.sub(r'\s{2,}', ' ', m.group(2).lstrip()), m.group(1))

    # step 4 - handle non-comma-introduced suffixes (e.g. "II") and compound last names (e.g.
    # Vaughan Williams)

    # step 5 - multiple names (e.g. "/" or "&" or "and" or ",")

    # step 6 - remove conductor role suffix ("cond.", "conductor", etc.)
    m = re.match(r'(.+), ([\w\./ ]+)$', cond_str)
    if m:
        if m.group(2).lower() in COND_STRS:
            log.debug("PDS_RULE 5 - removing role suffix \"%s\" for \"%s\"" %
                      (m.group(2), cond_str))
            cond_str = m.group(1)

    # step N - finally...
    if suffix:
        cond_str = cond_str.replace(SUFFIX_TOKEN, suffix, 1)

    cond_data = mkcond(cond_str, orig_cond_str)
    return ml_dict({'conductor': cond_data})

def parse_performer_str(perf_str, flags = None):
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
    orig_perf_str = perf_str

    def parse_perf_item(perf_item, fld_delim = ','):
        sub_data = []
        if perf_item.count(fld_delim) % 2 == 1:
            fields = perf_item.split(fld_delim)
            while fields:
                pers, role = (fields.pop(0), fields.pop(0))
                # special case for "<ens>/<cond last, first>"
                if pers.count('/') == 1:
                    log.debug("PPS_RULE 1 - slash separating ens from cond_last \"%s\"" % (pers))
                    ens_name, cond_last = pers.split('/')
                    cond_name = "%s %s" % (role, cond_last)
                    sub_data.append(mkperf(ens_name, 'ensemble', orig_perf_str))
                    sub_data.append(mkperf(cond_name, 'conductor', orig_perf_str))
                else:
                    sub_data.append(mkperf(pers, role, orig_perf_str))
        else:
            # TODO: if even number of field delimiters, need to look closer at item
            # contents/format to figure out what to do!!!
            sub_data.append(mkperf(perf_item, None, orig_perf_str))
        return {'performers': sub_data}

    ens_data  = []
    perf_data = []
    ret_data  = ml_dict({'ensembles': ens_data, 'performers': perf_data})
    # TODO: should really move the quote processing as far upstream as possible (for
    # all fields); NOTE: also need to revisit normalize_* functions in musiclib!!!
    m = re.match(r'"([^"]*)"$', perf_str)
    if m:
        log.debug("PPS_RULE 2 - strip enclosing quotes \"%s\"" % (perf_str))
        perf_str = m.group(1)  # note: could be empty string, handle downstream!
    m = re.match(r'\((.*[^)])\)?$', perf_str)
    if m:
        log.debug("PPS_RULE 3 - strip enclosing parens \"%s\"" % (perf_str))
        perf_str = m.group(1)  # note: could be empty string, handle downstream!
    # special case for ugly record (WNED 2018-09-17)
    m = re.match(r'(.+?)\r', perf_str)
    if m:
        log.debug("PPS_RULE 4 - ugly broken record for WNED \"%s\"" % (perf_str))
        perf_str = m.group(1)
        m = re.match(r'(.+)\[(.+)\],(.+)', perf_str)
        if m:
            perf_str = '; '.join(m.groups())

    if re.match(r'\/.+ \- ', perf_str):
        log.debug("PPS_RULE 5 - leading slash for performer fields \"%s\"" % (perf_str))
        for perf_item in perf_str.split('/'):
            if perf_item:
                ret_data.merge(parse_perf_item(perf_item, ' - '))
    elif ';' in perf_str:
        log.debug("PPS_RULE 6 - semi-colon-deliminted performer fields \"%s\"" % (perf_str))
        for perf_item in perf_str.split(';'):
            if perf_item:
                ret_data.merge(parse_perf_item(perf_item))
    elif perf_str:
        ret_data.merge(parse_perf_item(perf_str))

    return ret_data

def parse_ensemble_str(ens_str, flags = None):
    """
    :param ens_str:
    :param flags: (not yet implemented)
    :return: dict of ens_data/perf_data structures, indexed by type
    """
    orig_ens_str = ens_str

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
                    sub_ens_data.append(mkens(name, orig_ens_str))
                else:
                    sub_perf_data.append(mkperf(name, role, orig_ens_str))
        else:
            # TODO: if even number of field delimiters, need to look closer at item
            # contents/format to figure out what to do (i.e. NER)!!!
            sub_ens_data.append(mkens(ens_item, orig_ens_str))
        return {'ensembles' : sub_ens_data, 'performers': sub_perf_data}

    def parse_ens_fields(fields):
        sub_ens_data = []
        sub_perf_data = []
        while fields:
            if len(fields) == 1:
                sub_ens_data.append(mkens(fields.pop(0), orig_ens_str))
                break  # same as continue
            # more reliable to do this moving backward from the end (sez me)
            if ' ' not in fields[-1]:
                # REVISIT: we presume a single-word field to be a city/location (for now);
                # as above, we should really look at field contents to properly parse!!!
                ens = ','.join([fields.pop(-2), fields.pop(-1)])
                sub_ens_data.append(mkens(ens, orig_ens_str))
            else:
                # yes, do this twice!
                sub_ens_data.append(mkens(fields.pop(-1), orig_ens_str))
                sub_ens_data.append(mkens(fields.pop(-1), orig_ens_str))
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
        ens_data.append(mkens(ens_str, orig_ens_str))

    return ret_data

##################
# MusicEnt class #
##################

class MusicEnt(object):
    def __init__(self, entity):
        """
        :param entity: [string] name of entity (same as table name)
        """
        self.name = entity
        self.tab  = db.get_table(self.name)
        self.cols = set([c.name for c in self.tab.columns])

    def select(self, crit):
        """
        :param crit: dict of query criteria
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
            # the query crit, not sure why--we don't currently need that any more, but the error may
            # come up again in the future, so will need invested again then
            sel = sel.where(self.tab.c[col] == val)
        with db.conn.begin() as trans:
            res = db.conn.execute(sel)
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

        ins = self.tab.insert()
        with db.conn.begin() as trans:
            res = db.conn.execute(ins, data)
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
    @staticmethod
    def insert_program_play(playlist, data):
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
            log.debug("Inserting station \"%s\" into musiclib" % (station.name))
            ins_res = sta.insert(sta_data)
            if ins_res.rowcount == 0:
                raise RuntimeError("Could not insert station \"%s\" into musiclib" % (station.name))
            sta_row = sta.inserted_row(ins_res)
            if not sta_row:
                raise RuntimeError("Station %s not in musiclib" % (station.name))

        prog_data = data['program']
        prog = get_entity('program')
        sel_res = prog.select(key_data(prog_data, 'program'))
        if sel_res.rowcount == 1:
            prog_row = sel_res.fetchone()
        else:
            prog_name = prog_data['name']  # for convenience
            prog_label = "\"%s\"" % (prog_name)
            log.debug("Inserting program %s into musiclib" % (prog_label))
            ins_res = prog.insert(prog_data)
            if ins_res.rowcount == 0:
                raise RuntimeError("Could not insert program %s into musiclib" % (prog_label))
            prog_row = prog.inserted_row(ins_res)
            if not prog_row:
                raise RuntimeError("Program %s not in musiclib" % (prog_label))

        pp_row = None
        pp_data = data['program_play']
        pp_data['station_id'] = sta_row.id
        pp_data['program_id'] = prog_row.id
        prog_play = get_entity('program_play')
        try:
            ins_res = prog_play.insert(pp_data)
            pp_row = prog_play.inserted_row(ins_res)
            log.debug("Created program_play ID %d (%s, \"%s\", %s %s)" %
                      (pp_row.id, sta_row.name, prog_row.name,
                       pp_row.prog_play_date, pp_row.prog_play_start))
        except IntegrityError:
            # TODO: need to indicate duplicate to caller (currenty looks like an insert)!!!
            sel_res = prog_play.select(key_data(pp_data, 'program_play'))
            if sel_res.rowcount == 1:
                pp_row = sel_res.fetchone()
                log.debug("Skipping insert of duplicate program_play record (ID %d)" % (pp_row.id))
            else:
                pass  # REVISIT: is this an internal error???
        return {k: v for k, v in pp_row.items()} if pp_row else None

    @staticmethod
    def insert_play(playlist, prog_play, data):
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
            log.debug("Inserting station \"%s\" into musiclib" % (station.name))
            ins_res = sta.insert(sta_data)
            if ins_res.rowcount == 0:
                raise RuntimeError("Could not insert station \"%s\" into musiclib" % (station.name))
            sta_row = sta.inserted_row(ins_res)
            if not sta_row:
                raise RuntimeError("Station %s not in musiclib" % (station.name))

        comp_data = data['composer']
        # NOTE: we always make sure there is a composer record (even if NONE or UNKNOWN), since work depends
        # on it (and there is no play without work, haha)
        if not comp_data.get('name'):
            comp_data['name'] = NameVal.NONE
        comp = get_entity('person')
        sel_res = comp.select(key_data(comp_data, 'person'))
        if sel_res.rowcount == 1:
            comp_row = sel_res.fetchone()
        else:
            comp_name = comp_data['name']  # for convenience
            log.debug("Inserting composer \"%s\" into musiclib" % (comp_name))
            ins_res = comp.insert(comp_data)
            if ins_res.rowcount == 0:
                raise RuntimeError("Could not insert composer/person \"%s\" into musiclib" % (comp_name))
            comp_row = comp.inserted_row(ins_res)
            if not comp_row:
                raise RuntimeError("Composer/person \"%s\" not in musiclib" % (comp_name))

        work_data = data['work']
        if not work_data['name']:
            log.debug("Work name not specified, skipping...")
            return None
        work_data['composer_id'] = comp_row.id
        work = get_entity('work')
        sel_res = work.select(key_data(work_data, 'work'))
        if sel_res.rowcount == 1:
            work_row = sel_res.fetchone()
        else:
            work_name = work_data['name']  # for convenience
            log.debug("Inserting work \"%s\" into musiclib" % (work_name))
            ins_res = work.insert(work_data)
            if ins_res.rowcount == 0:
                raise RuntimeError("Could not insert work/person \"%s\" into musiclib" % (work_name))
            work_row = work.inserted_row(ins_res)
            if not work_row:
                raise RuntimeError("Work/person \"%s\" not in musiclib" % (work_name))

        cond_row = None
        cond_data = data['conductor']
        if cond_data.get('name'):
            cond = get_entity('person')
            sel_res = cond.select(key_data(cond_data, 'person'))
            if sel_res.rowcount == 1:
                cond_row = sel_res.fetchone()
            else:
                cond_name = cond_data['name']  # for convenience
                log.debug("Inserting conductor \"%s\" into musiclib" % (cond_name))
                ins_res = cond.insert(cond_data)
                if ins_res.rowcount == 0:
                    raise RuntimeError("Could not insert conductor/person \"%s\" into musiclib" % (cond_name))
                cond_row = cond.inserted_row(ins_res)
                if not cond_row:
                    raise RuntimeError("Conductor/person \"%s\" not in musiclib" % (cond_name))

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
                log.debug("Inserting recording \"%s\" into musiclib" % (rec_ident))
                ins_res = rec.insert(rec_data)
                if ins_res.rowcount == 0:
                    raise RuntimeError("Could not insert recording \"%s\" into musiclib" % (rec_ident))
                rec_row = rec.inserted_row(ins_res)
                if not rec_row:
                    raise RuntimeError("Recording \"%s\" not in musiclib" % (rec_ident))
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
                log.debug("Inserting recording \"%s\" into musiclib" % (rec_name))
                ins_res = rec.insert(rec_data)
                if ins_res.rowcount == 0:
                    raise RuntimeError("Could not insert recording \"%s\" into musiclib" % (rec_name))
                rec_row = rec.inserted_row(ins_res, 'recording_alt')
                if not rec_row:
                    raise RuntimeError("Recording \"%s\" not in musiclib" % (rec_name))

        perf_rows = []
        for perf_data in data['performers']:
            # STEP 1 -: insert/select underlying person record
            perf_person = get_entity('person')  # cached, so okay to re-get for each loop
            sel_res = perf_person.select(key_data(perf_data['person'], 'person'))
            if sel_res.rowcount == 1:
                perf_person_row = sel_res.fetchone()
            else:
                perf_name = perf_data['person']['name']  # for convenience
                log.debug("Inserting performer/person \"%s\" into musiclib" % (perf_name))
                ins_res = perf_person.insert(perf_data['person'])
                if ins_res.rowcount == 0:
                    raise RuntimeError("Could not insert performer/person \"%s\" into musiclib" % (perf_name))
                perf_person_row = perf_person.inserted_row(ins_res)
                if not perf_person_row:
                    raise RuntimeError("Performer/person \"%s\" not in musiclib" % (perf_name))
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
                log.debug("Inserting performer %s into musiclib" % (perf_label))
                ins_res = perf.insert(entity_data(perf_data, 'performer'))
                if ins_res.rowcount == 0:
                    raise RuntimeError("Could not insert performer %s into musiclib" % (perf_label))
                perf_row = perf.inserted_row(ins_res)
                if not perf_row:
                    raise RuntimeError("Performer %s not in musiclib" % (perf_label))
            perf_rows.append(perf_row)

        ens_rows = []
        for ens_data in data['ensembles']:
            ens = get_entity('ensemble')  # cached, so okay to re-get for each loop
            sel_res = ens.select(key_data(ens_data, 'ensemble'))
            if sel_res.rowcount == 1:
                ens_row = sel_res.fetchone()
            else:
                ens_name = ens_data['name']  # for convenience
                log.debug("Inserting ensemble \"%s\" into musiclib" % (ens_name))
                ins_res = ens.insert(ens_data)
                if ins_res.rowcount == 0:
                    raise RuntimeError("Could not insert ensemble \"%s\" into musiclib" % (ens_name))
                ens_row = ens.inserted_row(ins_res)
                if not ens_row:
                    raise RuntimeError("Ensemble \"%s\" not in musiclib" % (ens_name))
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
            log.debug("Created play ID %d (%s, \"%s\", %s %s)" %
                      (play_row.id, comp_row.name, work_row.name,
                       play_row.play_date, play_row.play_start))
        except IntegrityError:
            # TODO: need to indicate duplicate to caller (currenty looks like an insert)!!!
            log.debug("Skipping insert of duplicate play record:\n%s" % (play_data))
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
                    log.debug("Skipping insert of duplicate play_performer record:\n%s" % (play_perf_data))

            for ens_row in ens_rows:
                play_ens_data = {'play_id': play_row.id, 'ensemble_id': ens_row.id}
                play_ens = get_entity('play_ensemble')
                try:
                    ins_res = play_ens.insert(play_ens_data)
                    play_ens_rows.append(play_ens.inserted_row(ins_res))
                except IntegrityError:
                    log.debug("Skipping insert of duplicate play_ensemble record:\n%s" % (play_ens_data))

        return {k: v for k, v in play_row.items()}

    @staticmethod
    def insert_play_seq(play_rec, play_seq, hash_type):
        """
        :param play_rec:
        :param prog_seq:
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
                log.debug("Could not insert play_seq %s into musiclib" % (data))

        return ret

    @staticmethod
    def insert_entity_strings(playlist, data):
        """
        :return: list of key-value dict comprehensions for inserted entity_string fields
        """
        ctx = playlist.parse_ctx
        ret = []
        es = get_entity('entity_string')
        for entity_src, src_data in data['entity_str'].items():
            for entity_str in src_data:
                if not (entity_str and re.search('\w', entity_str)):
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
                    log.debug("Duplicate entity_string \"%s\" [%s] for station ID %d" %
                              (entity_str, entity_src, ctx['station_id']))

        return ret

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
    print("Rowcount: %d" % (res.rowcount))
    if res.returns_rows:
        print(res.fetchall())
    else:
        print(res.__dict__)

# -*- coding: utf-8 -*-

"""Music Library module
"""

from __future__ import absolute_import, division, print_function

import sys
import datetime as dt

from sqlalchemy import bindparam
from sqlalchemy.sql import func
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.types import DateTime
from sqlalchemy.exc import *

import core
from database import DatabaseCtx
from utils import LOV, prettyprint

#####################
# core/config stuff #
#####################

# shared resources from core
BASE_DIR     = core.BASE_DIR
cfg          = core.cfg
log          = core.log
sess         = core.sess
dflt_hand    = core.dflt_hand
dbg_hand     = core.dbg_hand
FETCH_INT    = core.FETCH_INT
FETCH_DELTA  = core.FETCH_DELTA

##############################
# common constants/functions #
##############################

# Lists of Values
NameVal = LOV({'NONE'   : '<none>',
               'UNKNOWN': '<unknown>'})

db = DatabaseCtx('dev')
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
    'play_seq'      : ['hash_level', 'hash_type', 'play_id']
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

#################
# Parsing logic #
#################

COND_STRS = set(['conductor',
                 'cond.',
                 'cond'])

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
            log.debug("Inserting program \"%s\" into musiclib" % (prog_name))
            ins_res = prog.insert(prog_data)
            if ins_res.rowcount == 0:
                raise RuntimeError("Could not insert program \"%s\" into musiclib" % (prog_name))
            prog_row = prog.inserted_row(ins_res)
            if not prog_row:
                raise RuntimeError("Program \"%s\" not in musiclib" % (prog_name))

        pp_row = None
        pp_data = data['program_play']
        pp_data['station_id'] = sta_row.id
        pp_data['program_id'] = prog_row.id
        prog_play = get_entity('program_play')
        try:
            ins_res = prog_play.insert(pp_data)
            pp_row = prog_play.inserted_row(ins_res)
        except IntegrityError:
            # TODO: need to indicate duplicate to caller (currenty looks like an insert)!!!
            log.debug("Skipping insert of duplicate program_play record")
            sel_res = prog_play.select(key_data(pp_data, 'program_play'))
            if sel_res.rowcount == 1:
                pp_row = sel_res.fetchone()
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
        if not comp_data['name']:
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
        if cond_data['name']:
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
                if perf_role:
                    perf_name += " (%s)" % (perf_role)
                log.debug("Inserting performer \"%s\" into musiclib" % (perf_name))
                ins_res = perf.insert(entity_data(perf_data, 'performer'))
                if ins_res.rowcount == 0:
                    raise RuntimeError("Could not insert performer \"%s\" into musiclib" % (perf_name))
                perf_row = perf.inserted_row(ins_res)
                if not perf_row:
                    raise RuntimeError("Performer \"%s\" not in musiclib" % (perf_name))
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
        except IntegrityError:
            # TODO: need to indicate duplicate to caller (currenty looks like an insert)!!!
            log.debug("Skipping insert of duplicate play record:\n%s" % (play_data))
            sel_res = play.select(key_data(play_data, 'play'))
            if sel_res.rowcount == 1:
                play_row = sel_res.fetchone()

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

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

import core
from database import DatabaseCtx
from utils import prettyprint, collecttype

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

db = DatabaseCtx('dev')
ml_cache = {}

def get_handle(entity):
    if entity in ml_cache:
        return ml_cache[entity]
    handle = MusicLib(entity)
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
    'play_ensemble' : ['play_id', 'ensemble_id']
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
# MusicLib class #
##################

class MusicLib(object):
    def __init__(self, entity):
        self.ent = entity
        self.tab = db.get_table(self.ent)
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
            raise RuntimeError("Unknown column(s) for \"%s\": %s" % (self.ent, str(unknown)))
        sel = self.tab.select()
        for col, val in crit.items():
            # REVISIT: don't why this doesn't work, but we don't really need it; the better
            # overall fix is specifying only the consequential/defining keys when requerying
            # a newly inserted row!!!
            if isinstance(val, dt.timedelta):
                continue
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
        tab = db.get_table('station')
        unknown = set(data) - self.cols
        if unknown:
            raise RuntimeError("Unknown column(s) for \"%s\": %s" % (self.ent, str(unknown)))

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
            sel_res = self.select({k: params[k] for k in params.viewkeys() & user_keys[ent_override]})
        else:
            sel_res = self.select({k: params[k] for k in params.viewkeys() & user_keys[self.ent]})
        #sel_res = self.select(params)
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

#####################
# command line tool #
#####################

if __name__ == '__main__':
    if len(sys.argv) < 4:
        raise RuntimeError("Usage: musiclib.py [select|insert] <entity> <key>=<value> ...")

    ml = MusicLib(sys.argv[2])
    meth = getattr(ml, sys.argv[1])
    data = {}
    for cond in sys.argv[3:]:
        (key, val) = cond.split('=')
        data[key] = int(val) if val.isdigit() else val
    res = meth(data)
    if collecttype(res):
        for x in res:
            print(x)
    else:
        print(res)

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

import station
from database import DatabaseCtx
from utils import prettyprint, collecttype

##############################
# common constants/functions #
##############################

# shared resources from station
cfg       = station.cfg
log       = station.log
sess      = station.sess
dflt_hand = station.dflt_hand
dbg_hand  = station.dbg_hand

db = DatabaseCtx('dev')
ml_cache = {}

def get_handle(entity):
    if entity in ml_cache:
        return ml_cache[entity]
    handle = MusicLib(entity)
    ml_cache[entity] = handle
    return handle

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

    def inserted_row(self, res):
        """
        :param res: SQLAlchemy ResultProxy from insert statement
        :return: SQLAlchemy RowProxy if exactly one row returned, otherwise None
        """
        params = res.last_inserted_params()
        sel_res = self.select(params)
        return sel_res.fetchone() if sel_res.rowcount == 1 else None

    def inserted_primary_key(self, res):
        """res.inserted_primary_key is not currently working (probably due to the use_identity() hack),
        so need to requery new row to get the primary key

        :param res: SQLAlchemy ResultProxy from insert statement
        :return: primary key of inserted row (or None, if row not [uniquely] identified)
        """
        ins_row = self.inserted_row(res)
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

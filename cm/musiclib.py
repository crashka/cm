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
import schema
from database import DatabaseCtx
from utils import LOV, prettyprint, str2date, str2time, collecttype

##############################
# common constants/functions #
##############################

INFO_KEYS    = set(['sta_name',
                    'datestr',
                    'name',
                    'status',
                    'file'])
NOPRINT_KEYS = set([])

# Lists of Values
Status = LOV(['NEW',
              'MISSING',
              'VALID',
              'INVALID',
              'DISABLED'], 'lower')

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
        self.entity = entity
        self.tab = db.get_table(entity)
        self.cols = set([c.name for c in self.tab.columns])

    def select(self, crit):
        if not crit:
            raise RuntimeError("Query criteria must be specified")
        unknown = set(crit) - self.cols
        if unknown:
            raise RuntimeError("Unknown column(s) for \"%s\": %s" % (self.ent, str(unknown)))
        sel = self.tab.select()
        for col, val in crit.items():
            sel = sel.where(self.tab.c[col] == val)

        with db.conn.begin() as trans:
            res = db.conn.execute(sel)
            rows = res.fetchall()
        return rows

    def insert(self, data):
        if not data:
            raise RuntimeError("Insert data must be specified")
        tab = db.get_table('station')
        unknown = set(data) - self.cols
        if unknown:
            raise RuntimeError("Unknown column(s) for \"%s\": %s" % (self.ent, str(unknown)))

        ins = self.tab.insert()
        with db.conn.begin() as trans:
            res = db.conn.execute(ins, data)
        return res.rowcount

def insert_program_play(station, data):
    """
    :param data: dict of playlist key/value fields
    :return: bool whether record was inserted
    """
    prog_info = data['program']
    data['date']        # 2018-09-19
    data['fullstart']   # 2018-09-19 12:00
    data['start_time']  # 12:00
    data['start_utc']   # Wed Sep 19 2018 12:00:00 GMT-0400 (EDT)
    data['fullend']     # 2018-09-19 13:00
    data['end_time']    # 13:00
    data['end_utc']     # Wed Sep 19 2018 13:00:00 GMT-0400 (EDT)

    (sdate, stime) = data['fullstart'].split()
    (edate, etime) = data['fullend'].split()
    if sdate != data['date']:
        log.debug("Date mismatch %s != %s" % (sdate, data['date']))
    if stime != data['start_time']:
        log.debug("Start time mismatch %s != %s" % (stime, data['start_time']))
    if etime != data['end_time']:
        log.debug("End time mismatch %s != %s" % (etime, data['end_time']))

    sta = get_handle('station')
    sta_res = sta.select({'name': station.name})
    if not sta_res:
        log.debug("Inserting station \"%s\" into musiclib" % (station.name))
        count = sta.insert({'name': station.name, 'timezone': station.time_zone})
        if count == 0:
            raise RuntimeError("Could not insert station \"%s\" into musiclib" % (station.name))
        sta_res = sta.select({'name': station.name})
        if not sta_res:
            raise RuntimeError("Station %s not in musiclib" % (station.name))

    prog = get_handle('program')
    prog_name = prog_info['name']
    prog_res = prog.select({'name': prog_name})
    if not prog_res:
        log.debug("Inserting program \"%s\" into musiclib" % (prog_name))
        count = prog.insert({'name': prog_name})
        if count == 0:
            raise RuntimeError("Could not insert program \"%s\" into musiclib" % (prog_name))
        prog_res = prog.select({'name': prog_name})
        if not prog_res:
            raise RuntimeError("Program \"%s\" not in musiclib" % (prog_name))

    pp_data = {}
    pp_data['station_id'] =        sta_res[0].id
    pp_data['program_id'] =        prog_res[0].id
    pp_data['prog_play_info'] =    data
    pp_data['prog_play_date'] =    str2date(sdate)
    pp_data['prog_play_start'] =   str2time(stime)
    pp_data['prog_play_end'] =     str2time(etime)
    pp_data['prog_play_dur'] =     None # Interval, if listed

    pp_data['notes'] =             None # ARRAY(Text)),

    pp_data['start_time'] =        None # TIMESTAMP(timezone=True)),
    pp_data['end_time'] =          None # TIMESTAMP(timezone=True)),
    pp_data['duration'] =          None # Interval)

    prog_play = get_handle('program_play')
    count = prog_play.insert(pp_data)
    return count == 1

def insert_play():
    """
    {
        "_date": "09182018",
        "_duration": 75000,
        "_end": "",
        "_end_datetime": "2018-09-19T02:01:15.000Z",
        "_end_time": "09-18-2018 23:01:15",
        "_err": [],
        "_id": "5b997fb0d40a7d62b2ebc048",
        "_source_song_id": "5b9009711941cf501dc736e3",
        "_start": "21:38:03",
        "_start_datetime": "2018-09-19T01:38:03.000Z",
        "_start_time": "09-18-2018 23:00:00",
        "artistName": "Emily Skala, flute",
        "buy": {},
        "catalogNumber": "324",
        "collectionName": "",
        "composerName": "Franz Schubert",
        "conductor": "",
        "copyright": "Summit",
        "ensembles": "",
        "episode_notes": "",
        "imageURL": "",
        "instruments": "PUvp",
        "program": "",
        "releaseDate": "2002",
        "soloists": "",
        "trackName": "Theme and Variations \"Dried Flowers\"",
        "trackNumber": "4-12",
        "upc": ""
    }
    """
    play.get('_start_time')
    pass

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

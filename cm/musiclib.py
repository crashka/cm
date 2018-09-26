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

#############################
# Entity-specific functions #
#############################

# REVISIT: to be encapsulated in a class structure???

def map_program_play(data):
    """This is the version for WWFM

    data in: 'onToday' item from WWFM playlist file
    data out: {
        'program': {},
        'program_play': {}
    }
    """
    prog_info = data['program']
    prog_name = prog_info['name']
    prog_data = {'name': prog_name}

    data.get('date')        # 2018-09-19
    data.get('fullstart')   # 2018-09-19 12:00
    data.get('start_time')  # 12:00
    data.get('start_utc')   # Wed Sep 19 2018 12:00:00 GMT-0400 (EDT)
    data.get('fullend')     # 2018-09-19 13:00
    data.get('end_time')    # 13:00
    data.get('end_utc')     # Wed Sep 19 2018 13:00:00 GMT-0400 (EDT)

    (sdate, stime) = data['fullstart'].split()
    (edate, etime) = data['fullend'].split()
    if sdate != data['date']:
        log.debug("Date mismatch %s != %s" % (sdate, data['date']))
    if stime != data['start_time']:
        log.debug("Start time mismatch %s != %s" % (stime, data['start_time']))
    if etime != data['end_time']:
        log.debug("End time mismatch %s != %s" % (etime, data['end_time']))

    pp_data = {}
    pp_data['prog_play_info'] =  data
    pp_data['prog_play_date'] =  str2date(sdate)
    pp_data['prog_play_start'] = str2time(stime)
    pp_data['prog_play_end'] =   str2time(etime)
    pp_data['prog_play_dur'] =   None # Interval, if listed
    pp_data['notes'] =           None # ARRAY(Text)),
    pp_data['start_time'] =      None # TIMESTAMP(timezone=True)),
    pp_data['end_time'] =        None # TIMESTAMP(timezone=True)),
    pp_data['duration'] =        None # Interval)

    return {'program': prog_data, 'program_play': pp_data}

def map_play(data):
    """This is the version for WWFM

    data in: 'playlist' item from WWFM playlist file
    data out: {
        'composer': {},
        'work': {},
        'performers': {},
        'ensembles': {},
        'conductor': {},
        'recording': {},
        'play': {}
    }
    """
    # do the easy stuff first (don't worry about empty records for now)
    composer_data  =  {'name'      : data.get('composerName')}
    work_data      =  {'name'      : data.get('trackName')}
    conductor_data =  {'name'      : data.get('conductor')}
    recording_data =  {'label'     : data.get('copyright'),
                       'catalog_no': data.get('catalogNumber'),
                       'name'      : data.get('collectionName')}

    # for performers, combine 'artistName' and 'soloists' (note that both/either can be
    # comma- or semi-colon-delimited)
    performers_data = []
    for perf_str in (data.get('artistName'), data.get('soloists')):
        if not perf_str:
            continue
        if ';' in perf_str:
            perfs = perf_str.split(';')
            for perf in perfs:
                fields = perf.rsplit(',', 1)
                if len(fields) == 1:
                    performers_data.append({'name': fields[0], 'role': None})
                else:
                    performers_data.append({'name': fields[0].strip(), 'role': fields[1].strip()})
        elif perf_str.count(',') % 2 == 1:
            fields = perf_str.split(',')
            while fields:
                pers, role = (fields.pop(0), fields.pop(0))
                performers_data.append({'name': pers.strip(), 'role': role.strip()})
        else:
            # TODO: if even number of commas, need to look closer at string contents/format
            # to figure out what to do!!!
            performers_data.append({'name': perf_str, 'role': None})

    # treat ensembles similar to performers, except no need to parse within semi-colon-delimited
    # fields, and slightly different logic for comma-delimited fields
    ensembles_data  =  []
    ensembles_str = data.get('ensembles')
    if ensembles_str:
        if ';' in ensembles_str:
            ensembles = ensembles_str.split(';')
            ensembles_data += [{'name': ens.strip()} for ens in ensembles]
        elif ',' in ensembles_str:
            fields = ensembles_str.split(',')
            while fields:
                if len(fields) == 1:
                    ensembles_data.append({'name': fields.pop(0).strip()})
                # more reliable to do this moving backward from the end (sez me)
                if ' ' not in fields[-1]:
                    # REVISIT: we presume a single-word field to be a city/location (for now);
                    # as above, we should really look at field contents to properly parse!!!
                    ens = ','.join([fields.pop(-2), fields.pop(-1)])
                    ensembles_data.append({'name': ens.strip()})
                else:
                    # yes, do this twice!
                    ensembles_data.append({'name': fields.pop(-1).strip()})
                    ensembles_data.append({'name': fields.pop(-1).strip()})
        else:
            ensembles_data.append({'name': ensembles_str})

    data.get('_id')              # 5b997ff162a4197540403ef5
    
    data.get('_date')            # 09202018
    data.get('_start')           # 02:10:20
    data.get('_start_time')      # 09-20-2018 03:10:20
    data.get('_start_datetime')  # 2018-09-20T06:10:20.000Z
    data.get('_end')             #
    data.get('_end_time')        # 09-20-2018 03:39:42
    data.get('_end_datetime')    # 2018-09-20T06:39:42.000Z
    data.get('_duration')        # 1762000 (msecs)

    data.get('composerName')     # Mauro Giuliani
    data.get('trackName')        # Guitar Concerto No. 3
    data.get('ensembles')        # Academy of St Martin in the Fields
    data.get('soloists')         #
    data.get('instruments')      # OXx
    data.get('artistName')       # Pepe Romero, guitar
    data.get('conductor')        # Neville Marriner

    data.get('copyright')        # Philips
    data.get('catalogNumber')    # 420780
    data.get('trackNumber')      # 4-6
    data.get('collectionName')   #
    data.get('releaseDate')      #
    data.get('upc')              #
    data.get('imageURL')         #
    data.get('program')          #
    data.get('episode_notes')    #
    data.get('_err')             # []

    (sdate, stime) = data['_start_time'].split()
    (edate, etime) = data['_end_time'].split()
    # NOTE: would like to do integrity check, but need to rectify formatting difference
    # for date, hour offset for time, non-empty value for _end!!!
    #if sdate != data['_date']:
    #    log.debug("Date mismatch %s != %s" % (sdate, data['date']))
    #if stime != data['_start']:
    #    log.debug("Start time mismatch %s != %s" % (stime, data['start_time']))
    #if etime != data['_end']:
    #    log.debug("End time mismatch %s != %s" % (etime, data['end_time']))

    dur_msecs = data.get('_duration')

    play_data = {}
    play_data['play_info'] =  data
    play_data['play_date'] =  str2date(sdate)
    play_data['play_start'] = str2time(stime)
    play_data['play_end'] =   str2time(etime)
    play_data['play_dur'] =   dt.timedelta(0, 0, 0, dur_msecs) if dur_msecs else None
    play_data['notes'] =      None # ARRAY(Text)),
    play_data['start_time'] = None # TIMESTAMP(timezone=True)),
    play_data['end_time'] =   None # TIMESTAMP(timezone=True)),
    play_data['duration'] =   None # Interval)

    return {
        'composer':   composer_data,
        'work':       work_data,
        'performers': performers_data,
        'ensembles':  ensembles_data,
        'conductor':  conductor_data,
        'recording':  recording_data,
        'play':       play_data
    }

def insert_program_play(station, data):
    """
    :param data: dict of playlist key/value fields
    :return: bool whether record was inserted
    """
    norm = map_program_play(data)

    sta = get_handle('station')
    sta_res = sta.select({'name': station.name})
    if sta_res.rowcount == 1:
        sta_row = sta_res.fetchone()
    else:
        log.debug("Inserting station \"%s\" into musiclib" % (station.name))
        ins_res = sta.insert({'name': station.name, 'timezone': station.time_zone})
        if ins_res.rowcount == 0:
            raise RuntimeError("Could not insert station \"%s\" into musiclib" % (station.name))
        sta_row = sta.inserted_row(ins_res)
        if not sta_row:
            raise RuntimeError("Station %s not in musiclib" % (station.name))

    prog_data = norm['program']
    prog_name = prog_data['name']
    prog = get_handle('program')
    prog_res = prog.select({'name': prog_name})
    if prog_res.rowcount == 1:
        prog_row = prog_res.fetchone()
    else:
        log.debug("Inserting program \"%s\" into musiclib" % (prog_name))
        ins_res = prog.insert(norm['program'])
        if ins_res.rowcount == 0:
            raise RuntimeError("Could not insert program \"%s\" into musiclib" % (prog_name))
        prog_row = prog.inserted_row(ins_res)
        if not prog_row:
            raise RuntimeError("Program \"%s\" not in musiclib" % (prog_name))

    pp_data = norm['program_play']
    pp_data['station_id'] = sta_row.id
    pp_data['program_id'] = prog_row.id
    prog_play = get_handle('program_play')
    res = prog_play.insert(pp_data)
    return prog_play.inserted_primary_key(res)

def insert_play(station, prog_play, data):
    """
    :param data: dict of playlist key/value fields
    :return: bool whether record was inserted
    """
    norm = map_program_play(data)

    sta = get_handle('station')
    sta_res = sta.select({'name': station.name})
    if not sta_res:
        log.debug("Inserting station \"%s\" into musiclib" % (station.name))
        res = sta.insert({'name': station.name, 'timezone': station.time_zone})
        if res.rowcount == 0:
            raise RuntimeError("Could not insert station \"%s\" into musiclib" % (station.name))
        sta_res = sta.select({'name': station.name})
        if not sta_res:
            raise RuntimeError("Station %s not in musiclib" % (station.name))

    prog_data = norm['program']
    prog_name = prog_data['name']
    prog = get_handle('program')
    prog_res = prog.select({'name': prog_name})
    if not prog_res:
        log.debug("Inserting program \"%s\" into musiclib" % (prog_name))
        res = prog.insert(norm['program'])
        if res.rowcount == 0:
            raise RuntimeError("Could not insert program \"%s\" into musiclib" % (prog_name))
        prog_res = prog.select({'name': prog_name})
        if not prog_res:
            raise RuntimeError("Program \"%s\" not in musiclib" % (prog_name))

    pp_data = norm['program_play']
    pp_data['station_id'] = sta_res[0].id
    pp_data['program_id'] = prog_res[0].id
    prog_play = get_handle('program_play')
    res = play.insert(pp_data)
    return play.inserted_primary_key(res)

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

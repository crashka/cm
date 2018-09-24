#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Database module
"""

from __future__ import absolute_import, division, print_function

import logging

from sqlalchemy import create_engine, MetaData
import click

import station
import schema

# shared resources from station
cfg       = station.cfg
log       = station.log
sess      = station.sess
dflt_hand = station.dflt_hand
dbg_hand  = station.dbg_hand

##############################
# common constants/functions #
##############################

eng  = create_engine("postgres://crash@/cmdev")
meta = MetaData(eng, reflect=True)

dblog = logging.getLogger('sqlalchemy.engine')
dblog.setLevel(logging.INFO)
dblog.addHandler(dflt_hand)

def create_schema(tables = None, dryrun = False, force = False):
    if len(meta.tables) > 0:
        raise RuntimeError("Schema must be empty, create is aborted")

    schema.load_schema(meta)
    if not tables or tables == 'all':
        meta.create_all()
    else:
        to_create = [meta.tables[tab] for tab in tables.split(',')]
        meta.create_all(tables=to_create)

def drop_schema(tables = None, dryrun = False, force = False):
    if len(meta.tables) == 0:
        raise RuntimeError("Schema is empty, nothing to drop")

    if not tables or tables == 'all':
        if not force:
            raise RuntimeError("Force must be specified if no list of tables given")
        meta.drop_all()
        meta.clear()
    else:
        to_drop = [meta.tables[tab] for tab in tables.split(',')]
        meta.drop_all(tables=to_drop)
        meta.clear()
        meta.reflect()

#####################
# command line tool #
#####################

@click.command()
@click.option('--list',     'cmd', flag_value='list', default=True, help="List all (or specified) tables for the database")
@click.option('--create',   'cmd', flag_value='create', help="Create specified table(s); NOTE: database must already be created")
@click.option('--drop',     'cmd', flag_value='drop', help="Drop specified table(s); NOTE, does not drop the actual database")
@click.option('--validate', 'cmd', flag_value='validate', help="Validate existing table(s) against current schema definition")
@click.option('--upgrade',  'cmd', flag_value='upgrade', help="Upgrade specified tables to new schema definition")
@click.option('--tables',   help="Table (or comma-separated list of tables) to operate on; defaults to 'all'")
@click.option('--force',    is_flag=True, help="Not currently implemented")
@click.option('--dryrun',   is_flag=True, help="Do not write changes to database, show SQL instead")
@click.option('--debug',    default=0, help="Debug level")
@click.argument('dbname',   default='dev', required=True)
def main(cmd, tables, force, dryrun, debug, dbname):
    """Manage database schema for specified DBNAME (defined in config file)
    """
    if debug > 0:
        log.setLevel(logging.DEBUG)
        log.addHandler(dbg_hand)
        # NOTE: DEBUG mode enables showing of query results (very verbose)
        #dblog.setLevel(logging.DEBUG)
        dblog.addHandler(dbg_hand)

    if cmd == 'list':
        pass
    elif cmd == 'create':
        create_schema(tables, dryrun, force)
    elif cmd == 'drop':
        drop_schema(tables, dryrun, force)
    elif cmd == 'validate':
        pass
    elif cmd == 'upgrade':
        pass

if __name__ == '__main__':
    main()

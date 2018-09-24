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

##############################
# common constants/functions #
##############################

# shared resources from station
cfg       = station.cfg
log       = station.log
sess      = station.sess
dflt_hand = station.dflt_hand
dbg_hand  = station.dbg_hand

DATABASE  = cfg.config('database')

dblog = logging.getLogger('sqlalchemy.engine')
dblog.setLevel(logging.INFO)
dblog.addHandler(dflt_hand)

####################
# Database Context #
####################

class DatabaseCtx(object):
    def __init__(self, dbname):
        if dbname not in DATABASE:
            raise RuntimeError("Database name \"%s\" not known" % (dbname))
        self.db_info = DATABASE[dbname]
        self.eng  = create_engine(self.db_info['connect_str'])
        self.meta = MetaData(self.eng, reflect=True)

    def create_schema(self, tables = None, dryrun = False, force = False):
        if len(self.meta.tables) > 0:
            raise RuntimeError("Schema must be empty, create is aborted")

        schema.load_schema(self.meta)
        if not tables or tables == 'all':
            self.meta.create_all()
        else:
            to_create = [self.meta.tables[tab] for tab in tables.split(',')]
            self.meta.create_all(tables=to_create)

    def drop_schema(self, tables = None, dryrun = False, force = False):
        if len(self.meta.tables) == 0:
            raise RuntimeError("Schema is empty, nothing to drop")

        if not tables or tables == 'all':
            if not force:
                raise RuntimeError("Force must be specified if no list of tables given")
            self.meta.drop_all()
            self.meta.clear()
        else:
            to_drop = [self.meta.tables[tab] for tab in tables.split(',')]
            self.meta.drop_all(tables=to_drop)
            self.meta.clear()
            self.meta.reflect()

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

    db = DatabaseCtx(dbname)

    if cmd == 'list':
        pass
    elif cmd == 'create':
        db.create_schema(tables, dryrun, force)
    elif cmd == 'drop':
        db.drop_schema(tables, dryrun, force)
    elif cmd == 'validate':
        pass
    elif cmd == 'upgrade':
        pass

if __name__ == '__main__':
    main()

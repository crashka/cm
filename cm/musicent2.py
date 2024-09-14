#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Music Entity module - Music Entities are mapped one-to-one with database tables, and
represent the components of the music library
"""

import datetime as dt
from typing import Optional, Annotated

from sqlalchemy import Integer, BigInteger, Text, Identity, func, text
from sqlalchemy import UniqueConstraint, ForeignKeyConstraint, Index
from sqlalchemy.orm import DeclarativeBase, registry, Mapped, mapped_column, relationship
from sqlalchemy.dialects import postgresql
from sqlalchemy.dialects.postgresql import ARRAY, JSONB, TIMESTAMP
from sqlalchemy.schema import CreateTable

############################
# Type Aliases/Annotations #
############################

KeyType    = int
pk         = Annotated[KeyType, mapped_column(Identity(), primary_key=True)]
fk         = KeyType
fk_arr     = list[KeyType]
hash_val   = Annotated[int, mapped_column(BigInteger)]
synd_lev   = Annotated[int, mapped_column(server_default=text('10'))]
created_ts = Annotated[dt.datetime, mapped_column(server_default=func.now())]
updated_ts = Annotated[dt.datetime, mapped_column(onupdate=func.now())]

#############
# ModelBase #
#############

class ModelBase(DeclarativeBase):
    registry = registry(
        type_annotation_map={
            str:           Text,
            list[str]:     ARRAY(Text),
            list[KeyType]: ARRAY(Integer),
            dict:          JSONB,
            dt.datetime:   TIMESTAMP(timezone=True)
        }
    )

    @classmethod
    def get_subclasses(cls) -> type:
        for subclass in cls.__subclasses__():
            yield from subclass.get_subclasses()
            yield subclass
        return

##########
# Person #
##########

class Person(ModelBase):
    __tablename__  = "person"
    __table_args__ = (
        UniqueConstraint('name'),
        UniqueConstraint('full_name'),
        ForeignKeyConstraint(['cnl_person_id'], ['person.id'])
    )

    id:                Mapped[pk]
    name:              Mapped[str]            # basic normalization
    raw_name:          Mapped[Optional[str]]  # REVISIT: should this be mapped from dict???

    # parsed (and normalized???)
    prefix:            Mapped[Optional[str]]
    first_name:        Mapped[Optional[str]]
    middle_name:       Mapped[Optional[str]]
    last_name:         Mapped[Optional[str]]
    suffix:            Mapped[Optional[str]]
    full_name:         Mapped[Optional[str]]  # assembled from normalized name components
    tags:              Mapped[Optional[list[str]]]

    # denormalized flags
    is_composer:       Mapped[Optional[bool]]
    is_conductor:      Mapped[Optional[bool]]
    is_performer:      Mapped[Optional[bool]]

    # reference info
    is_canonical:      Mapped[Optional[bool]]
    cnl_person_id:     Mapped[Optional[fk]]   # points to self, if canonical
    arkiv_uri:         Mapped[Optional[str]]

    # system columns
    created_at:        Mapped[created_ts]
    updated_at:        Mapped[updated_ts]

    def __str__(self) -> str:
        return f"Person(id={self.id}, name=\"{self.name}\")"

#############
# Performer #
#############

class Performer(ModelBase):
    __tablename__  = "performer"
    __table_args__ = (
        UniqueConstraint('person_id', 'role'),
        ForeignKeyConstraint(['person_id'], ['person.id']),
        ForeignKeyConstraint(['cnl_performer_id'], ['performer.id'])
    )

    id:                Mapped[pk]
    person_id:         Mapped[fk]
    role:              Mapped[Optional[str]]  # instrument, voice, role, etc.
    raw_role:          Mapped[Optional[str]]
    cnl_performer_id:  Mapped[Optional[fk]]

    # system columns
    created_at:        Mapped[created_ts]
    updated_at:        Mapped[updated_ts]

############
# Ensemble #
############

class Ensemble(ModelBase):
    __tablename__  = "ensemble"
    __table_args__ = (
        UniqueConstraint('name'),
        ForeignKeyConstraint(['cnl_ensemble_id'], ['ensemble.id'])
    )

    id:                Mapped[pk]
    name:              Mapped[str]
    raw_name:          Mapped[Optional[str]]

    # parsed and normalized
    ens_type:          Mapped[Optional[str]]
    ens_name:          Mapped[Optional[str]]
    ens_location:      Mapped[Optional[str]]  # informational
    tags:              Mapped[Optional[list[str]]]

    # reference info
    is_canonical:      Mapped[Optional[bool]]
    cnl_ensemble_id:   Mapped[Optional[fk]]   # points to self, if canonical
    arkiv_uri:         Mapped[Optional[str]]

    # system columns
    created_at:        Mapped[created_ts]
    updated_at:        Mapped[updated_ts]

########
# Work #
########

class Work(ModelBase):
    __tablename__  = "work"
    __table_args__ = (
        UniqueConstraint('composer_id', 'name'),
        ForeignKeyConstraint(['composer_id'], ['person.id']),
        ForeignKeyConstraint(['cnl_work_id'], ['work.id'])
    )

    id:                Mapped[pk]
    composer_id:       Mapped[fk]
    name:              Mapped[str]
    raw_name:          Mapped[Optional[str]]

    # parsed and normalized
    work_type:         Mapped[Optional[str]]
    work_name:         Mapped[Optional[str]]
    work_key:          Mapped[Optional[str]]
    catalog_no:        Mapped[Optional[str]]  # i.e. op., K., BWV, etc.
    tags:              Mapped[Optional[list[str]]]

    # reference info
    is_canonical:      Mapped[Optional[bool]]
    cnl_work_id:       Mapped[Optional[fk]]   # points to self, if canonical
    arkiv_uri:         Mapped[Optional[str]]

    # system columns
    created_at:        Mapped[created_ts]
    updated_at:        Mapped[updated_ts]

#############
# Recording #
#############

class Recording(ModelBase):
    __tablename__  = "recording"
    __table_args__ = (
        # TODO: put a partial index on ('name', 'label')!!!
        #       CREATE UNIQUE INDEX recording_altkey
        #           ON recording (name, label)
        #        WHERE catalog_no IS NULL;
        UniqueConstraint('label', 'catalog_no'),
    )

    id:                Mapped[pk]
    name:              Mapped[Optional[str]]  # if null, need label/catalog_no
    label:             Mapped[Optional[str]]
    catalog_no:        Mapped[Optional[str]]
    release_date:      Mapped[Optional[dt.date]]
    arkiv_uri:         Mapped[Optional[str]]

    # system columns
    created_at:        Mapped[created_ts]
    updated_at:        Mapped[updated_ts]

###############
# Performance #
###############

class Performance(ModelBase):
    __tablename__  = "performance"
    __table_args__ = (
        UniqueConstraint('work_id', 'performer_ids', 'ensemble_ids', 'conductor_id'),
        ForeignKeyConstraint(['work_id'], ['work.id']),
        ForeignKeyConstraint(['conductor_id'], ['person.id']),
        ForeignKeyConstraint(['recording_id'], ['recording.id'])
    )

    id:                Mapped[pk]
    work_id:           Mapped[fk]
    performer_ids:     Mapped[Optional[fk_arr]]  # ForeignKey('performer.id')
    ensemble_ids:      Mapped[Optional[fk_arr]]  # ForeignKey('ensemble.id')
    conductor_id:      Mapped[Optional[fk]]
    recording_id:      Mapped[Optional[fk]]
    notes:             Mapped[Optional[list[str]]]

    # system columns
    created_at:        Mapped[created_ts]
    updated_at:        Mapped[updated_ts]

###########
# Station #
###########

class Station(ModelBase):
    __tablename__  = "station"
    __table_args__ = (
        UniqueConstraint('name'),
    )

    id:                Mapped[pk]
    name:              Mapped[str]
    timezone:          Mapped[str]                 # tzdata (Olson/IANA) format

    # the following are informational only
    location:          Mapped[Optional[str]]       # e.g. "<city>, <state>"
    frequency:         Mapped[Optional[str]]
    website:           Mapped[Optional[str]]

    # misc/external information
    tags:              Mapped[Optional[list[str]]]
    notes:             Mapped[Optional[list[str]]]
    ext_id:            Mapped[Optional[str]]
    ext_mstr_id:       Mapped[Optional[str]]

    # reference info
    synd_level:        Mapped[Optional[synd_lev]]  # 0-100 (default: 10)

    # system columns
    created_at:        Mapped[created_ts]
    updated_at:        Mapped[updated_ts]

###########
# Program #
###########

class Program(ModelBase):
    __tablename__  = "program"
    __table_args__ = (
        UniqueConstraint('name', 'host_name'),
        ForeignKeyConstraint(['station_id'], ['station.id']),
        ForeignKeyConstraint(['mstr_program_id'], ['program.id'])
    )

    id:                Mapped[pk]
    name:              Mapped[str]
    host_name:         Mapped[Optional[str]]
    is_syndicated:     Mapped[Optional[bool]]
    station_id:        Mapped[Optional[fk]]

    # misc/external information
    tags:              Mapped[Optional[list[str]]]
    notes:             Mapped[Optional[list[str]]]
    ext_id:            Mapped[Optional[str]]
    ext_mstr_id:       Mapped[Optional[str]]

    # reference info
    synd_level:        Mapped[Optional[int]]  # inherit from station
    mstr_program_id:   Mapped[Optional[fk]]   # not null if syndicated
    website:           Mapped[Optional[str]]

    # system columns
    created_at:        Mapped[created_ts]
    updated_at:        Mapped[updated_ts]

###############
# ProgramPlay #
###############

class ProgramPlay(ModelBase):
    __tablename__  = "program_play"
    __table_args__ = (
        UniqueConstraint('station_id', 'prog_play_date', 'prog_play_start', 'program_id'),
        ForeignKeyConstraint(['station_id'], ['station.id']),
        ForeignKeyConstraint(['program_id'], ['program.id']),
        ForeignKeyConstraint(['mstr_prog_play_id'], ['program_play.id'])
    )

    id:                Mapped[pk]
    station_id:        Mapped[fk]
    prog_play_info:    Mapped[dict]                    # normalized information
    prog_play_date:    Mapped[dt.date]                 # listed local date
    prog_play_start:   Mapped[dt.time]                 # listed local time
    prog_play_end:     Mapped[Optional[dt.time]]       # if listed
    prog_play_dur:     Mapped[Optional[dt.timedelta]]  # if listed

    # foreign key lookups (OPEN ISSUE: should we create additional metadata
    # at this level for associations to composers, performers, etc.???)
    program_id:        Mapped[Optional[fk]]
    mstr_prog_play_id: Mapped[Optional[fk]]            # not null if syndicated

    # misc/external information
    tags:              Mapped[Optional[list[str]]]
    notes:             Mapped[Optional[list[str]]]
    ext_id:            Mapped[Optional[str]]
    ext_mstr_id:       Mapped[Optional[str]]

    # technical
    start_time:        Mapped[Optional[dt.datetime]]
    end_time:          Mapped[Optional[dt.datetime]]
    duration:          Mapped[Optional[dt.timedelta]]

    # system columns
    created_at:        Mapped[created_ts]
    updated_at:        Mapped[updated_ts]

    # relationships
    station:           Mapped[Station] = relationship()
    program:           Mapped[Optional[Program]] = relationship()

########
# Play #
########

class Play(ModelBase):
    __tablename__  = "play"
    __table_args__ = (
        UniqueConstraint('station_id', 'play_date', 'play_start', 'work_id'),
        ForeignKeyConstraint(['station_id'], ['station.id']),
        ForeignKeyConstraint(['prog_play_id'], ['program_play.id']),
        ForeignKeyConstraint(['program_id'], ['program.id']),
        ForeignKeyConstraint(['composer_id'], ['person.id']),
        ForeignKeyConstraint(['work_id'], ['work.id']),
        ForeignKeyConstraint(['conductor_id'], ['person.id']),
        ForeignKeyConstraint(['recording_id'], ['recording.id']),
        ForeignKeyConstraint(['mstr_play_id'], ['play.id'])
    )

    id:                Mapped[pk]
    station_id:        Mapped[fk]
    prog_play_id:      Mapped[Optional[fk]]
    play_info:         Mapped[dict]                    # normalized information
    play_date:         Mapped[dt.date]                 # listed local date
    play_start:        Mapped[dt.time]                 # listed local time
    play_end:          Mapped[Optional[dt.time]]       # if listed
    play_dur:          Mapped[Optional[dt.timedelta]]  # if listed

    # foreign key lookups
    program_id:        Mapped[Optional[fk]]            # denorm from program_play (do we need this???)
    composer_id:       Mapped[Optional[fk]]
    work_id:           Mapped[Optional[fk]]
    # the following two are denorms of the intersect table (let's see if it
    # is worth maintaining these for convenience)
    performer_ids:     Mapped[Optional[fk_arr]]        # ForeignKey('performer.id')
    ensemble_ids:      Mapped[Optional[fk_arr]]        # ForeignKey('ensemble.id')
    # this also is a denorm, since conductor will be a role in the performer table
    conductor_id:      Mapped[Optional[fk]]
    # REVISIT: commenting this out for now, not sure if soloists really needs to be
    # distinct from performers
    #soloist_ids:       Mapped[fk_arr]                  # ForeignKey('performer.id')
    recording_id:      Mapped[Optional[fk]]
    mstr_play_id:      Mapped[Optional[fk]]            # not null if syndicated

    # misc/external information
    tags:              Mapped[Optional[list[str]]]
    notes:             Mapped[Optional[list[str]]]     # movement(s), arrangement info, etc.
    ext_id:            Mapped[Optional[str]]
    ext_mstr_id:       Mapped[Optional[str]]

    # technical
    start_time:        Mapped[Optional[dt.datetime]]
    end_time:          Mapped[Optional[dt.datetime]]
    duration:          Mapped[Optional[dt.timedelta]]

    # system columns
    created_at:        Mapped[created_ts]
    updated_at:        Mapped[updated_ts]

    # relationships
    program:           Mapped[Optional[Program]] = relationship()
    prog_play:         Mapped[Optional[ProgramPlay]] = relationship()

#################
# PlayPerformer #
#################

class PlayPerformer(ModelBase):
    __tablename__  = "play_performer"
    __table_args__ = (
        UniqueConstraint('play_id', 'performer_id'),
        ForeignKeyConstraint(['play_id'], ['play.id']),
        ForeignKeyConstraint(['performer_id'], ['performer.id']),
        ForeignKeyConstraint(['mstr_play_perf_id'], ['play_performer.id'])
    )

    id:                Mapped[pk]
    play_id:           Mapped[fk]
    performer_id:      Mapped[fk]
    mstr_play_perf_id: Mapped[Optional[fk]]  # not null if syndicated (denorm)
    notes:             Mapped[Optional[list[str]]]

    # system columns
    created_at:        Mapped[created_ts]
    updated_at:        Mapped[updated_ts]

################
# PlayEnsemble #
################

class PlayEnsemble(ModelBase):
    __tablename__  = "play_ensemble"
    __table_args__ = (
        UniqueConstraint('play_id', 'ensemble_id'),
        ForeignKeyConstraint(['play_id'], ['play.id']),
        ForeignKeyConstraint(['ensemble_id'], ['ensemble.id']),
        ForeignKeyConstraint(['mstr_play_ens_id'], ['play_ensemble.id'])
    )

    id:                Mapped[pk]
    play_id:           Mapped[fk]
    ensemble_id:       Mapped[fk]
    mstr_play_ens_id:  Mapped[Optional[fk]]  # not null if syndicated (denorm)
    notes:             Mapped[Optional[list[str]]]

    # system columns
    created_at:        Mapped[created_ts]
    updated_at:        Mapped[updated_ts]

###########
# PlaySeq #
###########

class PlaySeq(ModelBase):
    __tablename__  = "play_seq"
    __table_args__ = (
        UniqueConstraint('hash_level', 'hash_type', 'play_id'),
        ForeignKeyConstraint(['play_id'], ['play.id']),
        ForeignKeyConstraint(['station_id'], ['station.id']),
        ForeignKeyConstraint(['program_id'], ['program.id']),
        ForeignKeyConstraint(['prog_play_id'], ['program_play.id']),
        Index('play_seq_seq_hash', 'seq_hash')
    )

    id:                Mapped[pk]
    seq_hash:          Mapped[hash_val]
    hash_level:        Mapped[int]
    hash_type:         Mapped[int]
    play_id:           Mapped[fk]

    # denorms
    station_id:        Mapped[Optional[fk]]
    program_name:      Mapped[Optional[str]]
    program_id:        Mapped[Optional[fk]]
    prog_play_id:      Mapped[Optional[fk]]

    # system columns
    created_at:        Mapped[created_ts]
    updated_at:        Mapped[updated_ts]

################
# PlaySeqMatch #
################

class PlaySeqMatch(ModelBase):
    __tablename__  = "play_seq_match"
    __table_args__ = (
        ForeignKeyConstraint(['pub_start_play_id'], ['play.id']),
        ForeignKeyConstraint(['pub_end_play_id'], ['play.id']),
        ForeignKeyConstraint(['sub_start_play_id'], ['play.id']),
        ForeignKeyConstraint(['sub_end_play_id'], ['play.id']),
        ForeignKeyConstraint(['pub_station_id'], ['station.id']),
        ForeignKeyConstraint(['sub_station_id'], ['station.id'])
    )

    id:                Mapped[pk]
    seq_len:           Mapped[int]
    seq_time:          Mapped[Optional[int]]  # elapsed time
    pub_start_play_id: Mapped[fk]
    pub_end_play_id:   Mapped[fk]
    sub_start_play_id: Mapped[fk]
    sub_end_play_id:   Mapped[fk]

    # denorms from {pub,sub}_start_play_id
    pub_station_id:    Mapped[Optional[fk]]
    sub_station_id:    Mapped[Optional[fk]]
    pub_program_name:  Mapped[Optional[str]]

    # system columns
    created_at:        Mapped[created_ts]
    updated_at:        Mapped[updated_ts]

################
# EntityString #
################

class EntityString(ModelBase):
    __tablename__  = "entity_string"
    __table_args__ = (
        UniqueConstraint('entity_str', 'source_fld', 'station_id'),
        ForeignKeyConstraint(['station_id'], ['station.id']),
        ForeignKeyConstraint(['prog_play_id'], ['program_play.id']),
        ForeignKeyConstraint(['play_id'], ['play.id'])
    )

    id:                Mapped[pk]
    entity_str:        Mapped[str]
    # source type: program, composer, conductor, ensemble, performer, work, etc.
    source_fld:        Mapped[str]
    parsed_data:       Mapped[Optional[dict]]
    station_id:        Mapped[Optional[fk]]  # denorm
    prog_play_id:      Mapped[Optional[fk]]
    play_id:           Mapped[Optional[fk]]

    # system columns
    created_at:        Mapped[created_ts]
    updated_at:        Mapped[updated_ts]

#############
# EntityRef #
#############

class EntityRef(ModelBase):
    __tablename__  = "entity_ref"
    __table_args__ = (
        UniqueConstraint('entity_ref', 'entity_type', 'ref_source'),
        ForeignKeyConstraint(['mstr_entity_id'], ['entity_ref.id'])
    )

    id:                Mapped[pk]
    entity_ref:        Mapped[str]
    entity_type:       Mapped[str]
    ref_source:        Mapped[Optional[str]]
    addl_ref:          Mapped[Optional[str]]
    source_data:       Mapped[Optional[dict]]
    is_raw:            Mapped[Optional[bool]]
    # REVISIT: master entities are stored in the same table, for now!!!
    is_entity:         Mapped[Optional[bool]]
    mstr_entity_name:  Mapped[Optional[str]]
    mstr_entity_id:    Mapped[Optional[fk]]
    entity_strength:   Mapped[Optional[int]]  # experiemental, meaning TBD
    ref_strength:      Mapped[Optional[int]]  # experiemental, meaning TBD

    # system columns
    created_at:        Mapped[created_ts]
    updated_at:        Mapped[updated_ts]

############
# ToDoList #
############

class ToDoList(ModelBase):
    __tablename__  = "to_do_list"
    __table_args__ = (
        ForeignKeyConstraint(['station_id'], ['station.id']),
        ForeignKeyConstraint(['prog_play_id'], ['program_play.id']),
        ForeignKeyConstraint(['play_id'], ['play.id'])
    )

    id:                Mapped[pk]
    action:            Mapped[str]
    depends_on:        Mapped[Optional[str]]
    status:            Mapped[str]
    station_id:        Mapped[Optional[fk]]
    prog_play_id:      Mapped[Optional[fk]]
    play_id:           Mapped[Optional[fk]]

    # system columns
    created_at:        Mapped[created_ts]
    updated_at:        Mapped[updated_ts]

########
# main #
########

def main():
    meta = ModelBase.metadata
    for table in meta.sorted_tables:
        print(CreateTable(table).compile(dialect=postgresql.dialect()))

    """
    person_data = {'id': 100, 'name': "John Doe"}
    person = Person(**person_data)
    print(person)
    """

if __name__ == '__main__':
    main()

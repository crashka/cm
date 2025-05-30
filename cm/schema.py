# -*- coding: utf-8 -*-

"""Schema module (currently tied to SQLAlchemy, but the idea would be to define more
abstractly (though that may never really happen)
"""

from sqlalchemy import Table, Column, ForeignKey, UniqueConstraint, Index, text, func
from sqlalchemy import Integer, BigInteger, Text, Boolean, Date, Time, Interval
from sqlalchemy.dialects.postgresql import ARRAY, JSONB, TIMESTAMP

from .utils import LOV

Entity = LOV([
    # music library
    'PERSON',
    'PERFORMER',
    'ENSEMBLE',
    'WORK',
    'RECORDING',
    'PERFORMANCE',
    # internet radio
    'STATION',
    'PROGRAM',
    'PROGRAM_PLAY',
    'PLAY',
    'PLAY_PERFORMER',
    'PLAY_ENSEMBLE',
    # data science
    'PLAY_SEQ',
    'PLAY_SEQ_MATCH',
    'ENTITY_STRING',
    'ENTITY_REF',
    # administrative
    'TO_DO_LIST'], 'lower')

def load_schema(meta):
    """This is the definition of the database schema

    :param meta: metadata structure to add schema to
    :return: dict of Table specifications, indexed by name
    """
    return {
        Entity.PERSON: Table('person', meta,
            Column('id',                Integer,     primary_key=True),
            Column('name',              Text,        nullable=False),  # basic normalization
            # REVISIT: should raw_name be a JSONB???
            Column('raw_name',          Text,        nullable=True),

            # parsed (and normalized???)
            Column('prefix',            Text),
            Column('first_name',        Text),
            Column('middle_name',       Text),
            Column('last_name',         Text),
            Column('suffix',            Text),
            Column('full_name',         Text),       # assembled from normalized name components
            Column('tags',              ARRAY(Text)),

            # denormalized flags
            Column('is_composer',       Boolean),
            Column('is_conductor',      Boolean),
            Column('is_performer',      Boolean),

            # canonicality
            Column('is_canonical',      Boolean),
            Column('cnl_person_id',     Integer,     ForeignKey('person.id')),  # points to self, if canonical
            Column('arkiv_uri',         Text),

            # system columns
            Column('created_at',        TIMESTAMP(timezone=True), server_default=func.now()),
            Column('updated_at',        TIMESTAMP(timezone=True), onupdate=func.now()),

            # constraints/indexes
            UniqueConstraint('name'),
            UniqueConstraint('full_name')
        ),
        Entity.PERFORMER: Table('performer', meta,
            Column('id',                Integer,     primary_key=True),
            Column('person_id',         Integer,     ForeignKey('person.id'), nullable=False),
            Column('role',              Text,        nullable=True),  # instrument, voice, role, etc.
            Column('raw_role',          Text,        nullable=True),
            Column('cnl_performer_id',  Integer,     ForeignKey('performer.id')),

            # system columns
            Column('created_at',        TIMESTAMP(timezone=True), server_default=func.now()),
            Column('updated_at',        TIMESTAMP(timezone=True), onupdate=func.now()),

            # constraints/indexes
            UniqueConstraint('person_id', 'role')
        ),
        Entity.ENSEMBLE: Table('ensemble', meta,
            Column('id',                Integer,     primary_key=True),
            Column('name',              Text,        nullable=False),
            Column('raw_name',          Text,        nullable=True),

            # parsed and normalized
            Column('ens_type',          Text),
            Column('ens_name',          Text),
            Column('ens_location',      Text),       # informational
            Column('tags',              ARRAY(Text)),

            # canonicality
            Column('is_canonical',      Boolean),
            Column('cnl_ensemble_id',   Integer,     ForeignKey('ensemble.id')),  # points to self, if canonical
            Column('arkiv_uri',         Text),

            # system columns
            Column('created_at',        TIMESTAMP(timezone=True), server_default=func.now()),
            Column('updated_at',        TIMESTAMP(timezone=True), onupdate=func.now()),

            # constraints/indexes
            UniqueConstraint('name')
        ),
        Entity.WORK: Table('work', meta,
            Column('id',                Integer,     primary_key=True),
            Column('composer_id',       Integer,     ForeignKey('person.id'), nullable=False),
            Column('name',              Text,        nullable=False),
            Column('raw_name',          Text,        nullable=True),

            # parsed and normalized
            Column('work_type',         Text),
            Column('work_name',         Text),
            Column('work_key',          Text),
            Column('catalog_no',        Text),       # i.e. op., K., BWV, etc.
            Column('tags',              ARRAY(Text)),

            # canonicality
            Column('is_canonical',      Boolean),
            Column('cnl_work_id',       Integer,     ForeignKey('work.id')),  # points to self, if canonical
            Column('arkiv_uri',         Text),

            # system columns
            Column('created_at',        TIMESTAMP(timezone=True), server_default=func.now()),
            Column('updated_at',        TIMESTAMP(timezone=True), onupdate=func.now()),

            # constraints/indexes
            UniqueConstraint('composer_id', 'name')
        ),
        Entity.RECORDING: Table('recording', meta,
            Column('id',                Integer,     primary_key=True),
            Column('name',              Text,        nullable=True),  # if null, need label/catalog_no
            Column('label',             Text),
            Column('catalog_no',        Text),
            Column('release_date',      Date),
            Column('arkiv_uri',         Text),

            # system columns
            Column('created_at',        TIMESTAMP(timezone=True), server_default=func.now()),
            Column('updated_at',        TIMESTAMP(timezone=True), onupdate=func.now()),

            # constraints/indexes
            # TODO: put a partial index on ('name', 'label')!!!
            #       CREATE UNIQUE INDEX recording_altkey
            #           ON recording (name, label)
            #        WHERE catalog_no IS NULL;
            UniqueConstraint('label', 'catalog_no')
        ),
        Entity.PERFORMANCE: Table('performance', meta,
            Column('id',                Integer,     primary_key=True),
            Column('work_id',           Integer,     ForeignKey('work.id'), nullable=False),
            Column('performer_ids',     ARRAY(Integer)),  # ForeignKey('performer.id')
            Column('ensemble_ids',      ARRAY(Integer)),  # ForeignKey('ensemble.id')
            Column('conductor_id',      Integer,     ForeignKey('person.id')),
            Column('recording_id',      Integer,     ForeignKey('recording.id')),
            Column('notes',             ARRAY(Text)),

            # system columns
            Column('created_at',        TIMESTAMP(timezone=True), server_default=func.now()),
            Column('updated_at',        TIMESTAMP(timezone=True), onupdate=func.now()),

            # constraints/indexes
            UniqueConstraint('work_id', 'performer_ids', 'ensemble_ids', 'conductor_id')
        ),
        Entity.STATION: Table('station', meta,
            Column('id',                Integer,     primary_key=True),
            Column('name',              Text,        nullable=False),
            Column('timezone',          Text,        nullable=False),  # tzdata (Olson/IANA) format

            # the following are informational only
            Column('location',          Text),       # e.g. "<city>, <state>"
            Column('frequency',         Text),
            Column('website',           Text),

            # misc/external information
            Column('tags',              ARRAY(Text)),
            Column('notes',             ARRAY(Text)),
            Column('ext_id',            Text),
            Column('ext_mstr_id',       Text),

            # canonicality/analytics metadata
            Column('synd_level',        Integer,     server_default=text('10')),  # 0-100 (default: 10)

            # system columns
            Column('created_at',        TIMESTAMP(timezone=True), server_default=func.now()),
            Column('updated_at',        TIMESTAMP(timezone=True), onupdate=func.now()),

            # constraints/indexes
            UniqueConstraint('name')
        ),
        Entity.PROGRAM: Table('program', meta,
            Column('id',                Integer,     primary_key=True),
            Column('name',              Text,        nullable=False),
            Column('host_name',         Text),
            Column('is_syndicated',     Boolean),
            Column('station_id',        Integer,     ForeignKey('station.id'), nullable=True),

            # misc/external information
            Column('tags',              ARRAY(Text)),
            Column('notes',             ARRAY(Text)),
            Column('ext_id',            Text),
            Column('ext_mstr_id',       Text),

            # canonicality/analytics metadata
            Column('synd_level',        Integer),    # inherit from station
            Column('mstr_program_id',   Integer,     ForeignKey('program.id')),  # not null if syndicated
            Column('website',           Text),

            # system columns
            Column('created_at',        TIMESTAMP(timezone=True), server_default=func.now()),
            Column('updated_at',        TIMESTAMP(timezone=True), onupdate=func.now()),

            # constraints/indexes
            UniqueConstraint('name', 'host_name')
        ),
        Entity.PROGRAM_PLAY: Table('program_play', meta,
            Column('id',                Integer,     primary_key=True),
            Column('station_id',        Integer,     ForeignKey('station.id'), nullable=False),
            Column('prog_play_info',    JSONB,       nullable=False),  # normalized information
            Column('prog_play_date',    Date,        nullable=False),  # listed local date
            Column('prog_play_start',   Time,        nullable=False),  # listed local time
            Column('prog_play_end',     Time),       # if listed
            Column('prog_play_dur',     Interval),   # if listed

            # foreign key lookups (OPEN ISSUE: should we create additional metadata
            # at this level for associations to composers, performers, etc.???)
            Column('program_id',        Integer,     ForeignKey('program.id')),
            Column('mstr_prog_play_id', Integer,     ForeignKey('program_play.id')),  # not null if syndicated

            # misc/external information
            Column('tags',              ARRAY(Text)),
            Column('notes',             ARRAY(Text)),
            Column('ext_id',            Text),
            Column('ext_mstr_id',       Text),

            # technical
            Column('start_time',        TIMESTAMP(timezone=True)),
            Column('end_time',          TIMESTAMP(timezone=True)),
            Column('duration',          Interval),

            # system columns
            Column('created_at',        TIMESTAMP(timezone=True), server_default=func.now()),
            Column('updated_at',        TIMESTAMP(timezone=True), onupdate=func.now()),

            # constraints/indexes
            UniqueConstraint('station_id', 'prog_play_date', 'prog_play_start', 'program_id')
        ),
        Entity.PLAY: Table('play', meta,
            Column('id',                Integer,     primary_key=True),
            Column('station_id',        Integer,     ForeignKey('station.id'), nullable=False),
            Column('prog_play_id',      Integer,     ForeignKey('program_play.id'), nullable=True),
            Column('play_info',         JSONB,       nullable=False),  # normalized information
            Column('play_date',         Date,        nullable=False),  # listed local date
            Column('play_start',        Time,        nullable=False),  # listed local time
            Column('play_end',          Time),       # if listed
            Column('play_dur',          Interval),   # if listed

            # foreign key lookups
            Column('program_id',        Integer,     ForeignKey('program.id')),  # denorm from program_play (do we need this???)
            Column('composer_id',       Integer,     ForeignKey('person.id')),
            Column('work_id',           Integer,     ForeignKey('work.id')),
            # NOTE: typically, there will be either artist(s) OR ensemble(s)
            # (though there can also be both) conductors and soloists are
            # associated with ensembles
            # the following two are denorms of the intersect table (let's see if it
            # is worth maintaining these for convenience)
            Column('performer_ids',     ARRAY(Integer)),  # ForeignKey('performer.id')
            Column('ensemble_ids',      ARRAY(Integer)),  # ForeignKey('ensemble.id')
            # NOTE: this also is a denorm, since conductor will be a role in the performer table
            Column('conductor_id',      Integer,     ForeignKey('person.id')),
            # REVISIT: commenting this out for now, not sure if soloists really needs to be
            # distinct from performers
            #Column('soloist_ids',     ARRAY(Integer)),    # ForeignKey('performer.id')
            Column('recording_id',      Integer,     ForeignKey('recording.id')),
            Column('mstr_play_id',      Integer,     ForeignKey('play.id')),  # not null if syndicated

            # misc/external information
            Column('tags',              ARRAY(Text)),
            Column('notes',             ARRAY(Text)),  # movement(s), arrangement info, etc.
            Column('ext_id',            Text),
            Column('ext_mstr_id',       Text),

            # technical
            Column('start_time',        TIMESTAMP(timezone=True)),
            Column('end_time',          TIMESTAMP(timezone=True)),
            Column('duration',          Interval),

            # system columns
            Column('created_at',        TIMESTAMP(timezone=True), server_default=func.now()),
            Column('updated_at',        TIMESTAMP(timezone=True), onupdate=func.now()),

            # constraints/indexes
            UniqueConstraint('station_id', 'play_date', 'play_start', 'work_id')
        ),
        Entity.PLAY_PERFORMER: Table('play_performer', meta,
            Column('id',                Integer,     primary_key=True),
            Column('play_id',           Integer,     ForeignKey('play.id'), nullable=False),
            Column('performer_id',      Integer,     ForeignKey('performer.id'), nullable=False),
            Column('mstr_play_perf_id', Integer,     ForeignKey('play_performer.id')),  # not null if syndicated (denorm)
            Column('notes',             ARRAY(Text)),

            # system columns
            Column('created_at',        TIMESTAMP(timezone=True), server_default=func.now()),
            Column('updated_at',        TIMESTAMP(timezone=True), onupdate=func.now()),

            # constraints/indexes
            UniqueConstraint('play_id', 'performer_id')
        ),
        Entity.PLAY_ENSEMBLE: Table('play_ensemble', meta,
            Column('id',                Integer,     primary_key=True),
            Column('play_id',           Integer,     ForeignKey('play.id'), nullable=False),
            Column('ensemble_id',       Integer,     ForeignKey('ensemble.id'), nullable=False),
            Column('mstr_play_ens_id',  Integer,     ForeignKey('play_ensemble.id')),  # not null if syndicated (denorm)
            Column('notes',             ARRAY(Text)),

            # system columns
            Column('created_at',        TIMESTAMP(timezone=True), server_default=func.now()),
            Column('updated_at',        TIMESTAMP(timezone=True), onupdate=func.now()),

            # constraints/indexes
            UniqueConstraint('play_id', 'ensemble_id')
        ),
        Entity.PLAY_SEQ: Table('play_seq', meta,
            Column('id',                Integer,     primary_key=True),
            Column('seq_hash',          BigInteger,  nullable=False),
            Column('hash_level',        Integer,     nullable=False),
            Column('hash_type',         Integer,     nullable=False),
            Column('play_id',           Integer,     ForeignKey('play.id'), nullable=False),
            # denorms
            Column('station_id',        Integer,     ForeignKey('station.id')),
            Column('program_name',      Text),
            Column('program_id',        Integer,     ForeignKey('program.id')),
            Column('prog_play_id',      Integer,     ForeignKey('program_play.id')),

            # system columns
            Column('created_at',        TIMESTAMP(timezone=True), server_default=func.now()),
            Column('updated_at',        TIMESTAMP(timezone=True), onupdate=func.now()),

            # constraints/indexes
            UniqueConstraint('hash_level', 'hash_type', 'play_id'),
            Index('play_seq_seq_hash', 'seq_hash')
        ),
        Entity.PLAY_SEQ_MATCH: Table('play_seq_match', meta,
            Column('id',                Integer,     primary_key=True),
            Column('seq_len',           Integer,     nullable=False),
            Column('seq_time',          Integer),    # elapsed time
            Column('pub_start_play_id', Integer,     ForeignKey('play.id'), nullable=False),
            Column('pub_end_play_id',   Integer,     ForeignKey('play.id'), nullable=False),
            Column('sub_start_play_id', Integer,     ForeignKey('play.id'), nullable=False),
            Column('sub_end_play_id',   Integer,     ForeignKey('play.id'), nullable=False),
            # denorms from {pub,sub}_start_play_id
            Column('pub_station_id',    Integer,     ForeignKey('station.id')),
            Column('sub_station_id',    Integer,     ForeignKey('station.id')),
            Column('pub_program_name',  Text),

            # system columns
            Column('created_at',        TIMESTAMP(timezone=True), server_default=func.now()),
            Column('updated_at',        TIMESTAMP(timezone=True), onupdate=func.now())
        ),
        Entity.ENTITY_STRING: Table('entity_string', meta,
            Column('id',                Integer,     primary_key=True),
            Column('entity_str',        Text,        nullable=False),
            # source field (category): program, composer, conductor, ensemble, performer, work, etc.
            Column('source_fld',        Text,        nullable=False),
            Column('parsed_data',       JSONB),
            Column('station_id',        Integer,     ForeignKey('station.id')),  # denorm
            Column('prog_play_id',      Integer,     ForeignKey('program_play.id')),
            Column('play_id',           Integer,     ForeignKey('play.id')),

            # system columns
            Column('created_at',        TIMESTAMP(timezone=True), server_default=func.now()),
            Column('updated_at',        TIMESTAMP(timezone=True), onupdate=func.now()),

            # constraints/indexes
            UniqueConstraint('entity_str', 'source_fld', 'station_id')
        ),
        Entity.ENTITY_REF: Table('entity_ref', meta,
            Column('id',                Integer,     primary_key=True),
            Column('entity_ref',        Text,        nullable=False),
            Column('entity_type',       Text,        nullable=False),
            Column('ref_source',        Text,        nullable=True),
            Column('addl_ref',          Text,        nullable=True),
            Column('source_data',       JSONB),
            Column('is_raw',            Boolean),
            # REVISIT: master entities are stored in the same table, for now!!!
            Column('is_entity',         Boolean),
            Column('mstr_entity_name',  Text),
            Column('mstr_entity_id',    Integer,     ForeignKey('entity_ref.id')),
            Column('entity_strength',   Integer),    # experiemental, meaning TBD
            Column('ref_strength',      Integer),    # experiemental, meaning TBD

            # system columns
            Column('created_at',        TIMESTAMP(timezone=True), server_default=func.now()),
            Column('updated_at',        TIMESTAMP(timezone=True), onupdate=func.now()),

            # constraints/indexes
            UniqueConstraint('entity_ref', 'entity_type', 'ref_source')
        ),
        Entity.TO_DO_LIST: Table('to_do_list', meta,
            Column('id',                Integer,     primary_key=True),
            Column('action',            Text,        nullable=False),
            Column('depends_on',        Text),
            Column('status',            Text,        nullable=False),
            Column('station_id',        Integer,     ForeignKey('station.id')),
            Column('prog_play_id',      Integer,     ForeignKey('program_play.id')),
            Column('play_id',           Integer,     ForeignKey('play.id')),

            # system columns
            Column('created_at',        TIMESTAMP(timezone=True), server_default=func.now()),
            Column('updated_at',        TIMESTAMP(timezone=True), onupdate=func.now())
        )
    }

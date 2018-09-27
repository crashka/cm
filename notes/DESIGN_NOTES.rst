-------------------
Directory structure
-------------------

::

  cm/
    stations/
      WWFM/
        station_info.json
        playlists.json
        playlists/
          2018-09-06.json
    refdata/
      archivmusic/
        composers/
          a.html
          b.html
             .
             .
             .
        performers/
          ...
        conductors/
          ...
        ensembles/
          ...

-----------------
station_info.json
-----------------

::

  {
    name:      <name>,
    status:    <status>,
    config:    {...},
    state:     {...},
    playlists: {...},
    shows:     {... }
  }

``config`` fields (from config file):

* url_fmt - substition tokens as <TOKEN_NAME>; magic tokens (local variables or station attributes):
   * DATE_STR
* date_fmt - uses strftime tokens (e.g. %Y, %m, %d)
* time_zone - "tz" values
   * ``America/New_York``
   * ``America/Chicago``
   * ``America/Denver``
   * ``America/Los_Angeles``
* playlist_ext - currently: json or html

``state`` fields (written by validation process):

* pl_total (count)
* pl_earliest (Y-m-d)
* pl_latest (Y-m-d)
* pl_valid (count)
* pl_missing (count)
* pl_invalid (count)

``playlists`` schema

* currently same as playlists.json (see below), which  may be going away, as soon as validation logic is fully developed
* *[if/when playlists.json goes away, schema description should be moved here]*

--------------
playlists.json
--------------

::

  {
    <pl_date>: {
      file:   <filename>,
      status: <status value>
    },
    <pl_date>: {
      ...
    },
    ...
  }

Notes

* ``pl_date`` specified as Y-m-d format
* ``filename`` specified as relative pathname (from station directory)

**Status values**

* ``ok``
* ``failed``
* ``day_zero``

--------------
Station Module
--------------

**Commands:**

--list       List all (or specified) stations
--create     Create new station (skip if station exists)
--playlists  List playlists for station (fail if station does not exist)
--fetch      Fetch playlists for station (fail if station does not exist)
--validate   Validate playlist metadata for station (fail if station does not exist)

**Common flags:**

--skip       Skip (rather than fail) if station does not exist

**Playlist flags:**

--date=date  Start date to list, fetch, or validate
--num=num    Number of additional dates to list, fetch, or validate (positive indicates
             forward in time from start date, negative indicates backward in time), default: 0
--force      Overwrite existing playlists (otherwise skip over), applies only to fetch

**Arguments:**

:name:       Comma-separated list of names (or 'all'), default: all

-------------
Working Notes
-------------

**Station status values**

* **unknown** - not in config file
   * Currently: ``Station`` object constructor fails
* **created** - newly created (e.g. implicitly) but not yet validated
   * Station directory exists
   * ``station_info.json`` file exists
   * ``playlists`` directory exists
* **active** - created + validation
   * Station is created
   * Metadata is created and consistent
* **invalid** - validation fails (needs manual fixup before validation)
   * Metadata is found to be inconsistent
   * Must be manually fixed up and revalidated (for now)
      * Perhaps later: automated fixup operation
   * Otherwise similar to "disabled"
* **disabled** - manually disabled
   * Must be manually enabled and revalidated

**Creation process**

* Create station directory (fail if already exists)
* Create ``playlists`` sub-directory
* Create ``station_info.json``
   * Write ``name`` and ``status`` fields
   * Write ``config`` structure (from config file)
   * Empty structures for ``state`` and ``playlists`` (managed by validation process)
* TEMP: write ``playlists.json`` file

**Validation process**

* High-level validation:
   * Check existence and JSON integrity of ``station_info.json`` file
* Scan ``playlists`` sub-directory
   * Confirm (or update) ``playlists`` info structure
   * Record earliest and latest playlists, determine count, missing, etc.
   * Update ``state`` info structure
   * Set ``state`` value (either "active" or "invalid")
   * Write ``station_info.json`` file
   * TEMP: write ``playlists.json`` file
   * LATER: validate playlist contents, confirm/update playlist metadata

**Fetch playlists**

* Fetch targets:
   * **range** (i.e. start date + number)
   * **catchup** (all since latest in ``playlists``)
      * Fails if latest is either missing or invalid
   * **missing** (gaps between earliest and latest)
   * **invalid** (ignore if marked "dead" or "skip")
* Only fetch if station is "active"

**Parse playlists**

* Playlist info
   * ``programs`` (meaning a program plays/instances)
   * ``plays`` (possibly child of programs)
* Program info
   * start_datetime
   * end_datetime
   * duration
   * master_program (dbref)
   * prog_name
   * prog_notes
   * tags
   * plays(???)
* Play info
   * start_datetime
   * end_datetime
   * duration
   * data (blob)
   * fields_raw
   * fields_tagged
   * master_work (dbref)
   * master_rec (dbref)
   * master_comp (denorm from work)
   * master_perfs[] (dbrefs)
      * support "unknown" perf role
* looping through programs/plays
   * set current prog
   * set current play
   * parse out raw fields
      * normalize fields
      * tag fields
      * dblookups on fields

**parsing WWFM**
::

  pl_params = playlist['params']

  pl_progs = playlist['onToday']
  for prog in pl_progs:
    prog_info = prog.get('program')
    for play in prog.get('playlist'):
      assert(type(play) == dict)

  pl_param fields:
  {
      'date': '2018-09-14',
      'format': 'json'
  }

  prog fields:
  {
      '_id': '5b75c20045fee126f2ef53ae',
      '_syndication': {   'date': '09-05-2018 12:53:55',
                           'method': 'Song Stream'},
      'conflict_edited': 1534444032227,
      'conflicts': ['5b75c20045fee126f2ef53ae'],
      'date': '2018-09-14',
      'day': 'Fri',
      'end_time': '00:00',
      'end_utc': 'Sat Sep 15 2018 00:00:00 GMT-0400 (EDT)',
      'event_id': '5b75c20045fee126f2ef53a8',
      'fullend': '2018-09-15 00:00',
      'fullstart': '2018-09-14 23:00',
      'has_playlist': True,
      'playlist': [{...}, ...]
      'program': {...}
      'program_id': '5b75c20045fee126f2ef53a9',
      'start_time': '23:00',
      'start_utc': 'Fri Sep 14 2018 23:00:00 GMT-0400 (EDT)',
      'widget_config': {}
  }

  prog_info fields:
  {
      'facebook': '',
      'hosts': [],
      'isParent': False,
      'name': 'Classical Music with Scott Blankenship',
      'national_program_id': '',
      'parentID': '524c7ea2e1c85d374d5a2f25',
      'program_desc': '',
      'program_format': 'Classical',
      'program_id': '5b7af12e89d9bf2b60bf5fed',
      'program_link': '',
      'station_id': '',
      'twitter': '',
      'ucs': '53a98e36e1c80647e855fc88'
  }

  play fields:
  {
      '_date': '09132018',
      '_duration': 70000,
      '_end': '',
      '_end_datetime': '2018-09-14T02:01:10.000Z',
      '_end_time': '09-13-2018 23:01:10',
      '_err': [],
      '_id': '5b9009f61941cf501dc7596a',
      '_source_song_id': '5b90095e1941cf501dc7324d',
      '_start': '21:45:21',
      '_start_datetime': '2018-09-14T01:45:21.000Z',
      '_start_time': '09-13-2018 23:00:00',
      'artistName': '',
      'buy': {   },
      'catalogNumber': '555392',
      'collectionName': '',
      'composerName': 'Anton Rubinstein',
      'conductor': 'Stephen Gunzenhauser',
      'copyright': 'Naxos',
      'ensembles': 'Slovak Philharmonic Orchestra',
      'episode_notes': '',
      'imageURL': '',
      'instruments': 'O',
      'program': '',
      'releaseDate': '',
      'soloists': '',
      'trackName': 'Symphony No. 2 "Ocean": 7th movement',
      'trackNumber': '1-7',
      'upc': ''
  }

**Music lib**

* functions
   * dedup program/plays
   * dedup program fragments (sequences)
   * create metadata for one-off programs/plays
   * create masters for syndicated programs
   * create masters for weekly programs
   * create masters for syndicated plays
   * archivmusic lookups
* notes
   * template for parsing
      * DSL based on:
         * formatting
         * position
         * keywords/regexp
      * exception handling
         * place unparseable entries in quarantine
      * fuzzy matching (with confidence score) by field type
         * person
         * role (e.g. instrument, conductor, leader, etc.)
         * performer (person + role)
         * composer
         * piece (work)
         * recording (label + catno)
         * recording date (esp. for live)
         * program
      * on fuzzy match, queue to learning module (qualified by score)
        
**crontab entry**
::

  15 0 * * * /local/prod/cmprod/scripts/cm_station.sh --fetch --date=catchup --debug=1 all >> /local/prod/cmprod/log/cm_station.log 2>&1

**hash sequence matching**

* Have a canonical list of stations ranked from most generic (closest to syndicated
  programming) to most specialized, used to designate producer from subscriber for
  sequence matches

------------------------
To Do - Immediate/Active
------------------------

* "basic normalization" for person
* musiclib: recordings
* classify/fix anomalies for person (track fixes!!!)
* add C24
* add other staions using WWFM format
* play_seq
* play_seq_match

-------------------
To Do - Bugs/Tweaks
-------------------

* add ``--force`` flag to overwrite existing playlists
   * force pull all stations 09/13-09/18 due to previous tight (15 minute) cron window
* create backup files for ``station_info.json`` and ``playlists.json``

----------------
To Do - Features
----------------

* track **all** data fixups (whether manual or programmatic) so that they are re-applyable!!!
* **music module integrity**
* make logging (and printing for CLI commands) consistent
* write valid, missing, invalid to state structure
* fetch missing playlists
* validate playlist contents, record as metadata
* context manager for throttling playlist fetches
* job queue for playlist fetches (cycle through stations)
* get older playlists (determine epoch/beginning of time) for all stations
* archive function for playlists (and station info)
* Fork/port to python3 (rename to cmir)
* locate ``stations`` directory in ``config.yml`` (can be outside of cmir)

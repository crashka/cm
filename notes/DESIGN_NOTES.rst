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
          composers:a.html
          composers:b.html
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

**basic normalization for person**

* take context into account
* additional overrides for inclusion/exclusion of specific fixups
* fixups
   * field bracketed with quotes (may indicate complex field)
   * preserve "Jr.", "Sr.", etc. (factor out from regular comma processing)
   * fix "Last, First" (handle "Last, First Middle")
   * multiple names (e.g. "/" or "&" or "and" or ",")
   * "arr.", "arranged", "orch.", "orchestrated", etc. (for composer)

----------------
Playlist Parsing
----------------

**composer names to normalize (basic)**
::

                 name                |                raw_name                 
  -----------------------------------+-----------------------------------------
   Anonymous 16th century, Scottish  | "\"Anonymous 16th century, Scottish\""
   Auber, Daniel-Franï¿½ois          | "\"Auber, Daniel-Franï¿½ois\""
   Bartï¿½k, Bï¿½la                  | "\"Bartï¿½k, Bï¿½la\""
   Bï¿½riot, Charles Auguste de      | "\"Bï¿½riot, Charles Auguste de\""
   Borne, Franï¿½ois                 | "\"Borne, Franï¿½ois\""
   Chopin, Frï¿½dï¿½ric              | "\"Chopin, Frï¿½dï¿½ric\""
   Dï¿½libes, Lï¿½o                  | "\"Dï¿½libes, Lï¿½o\""
   Dohnï¿½nyi, Ernst von             | "\"Dohnï¿½nyi, Ernst von\""
   Dvorï¿½k, Antonï¿½n               | "\"Dvorï¿½k, Antonï¿½n\""
   Franï¿½aix, Jean                  | "\"Franï¿½aix, Jean\""
   Friedrich II, Frederick the Great | "\"Friedrich II, Frederick the Great\""
   Hï¿½ffner, Anton                  | "\"Hï¿½ffner, Anton\""
   Hubay, Jenï¿½                     | "\"Hubay, Jenï¿½\""
   Kodï¿½ly, Zoltï¿½n                | "\"Kodï¿½ly, Zoltï¿½n\""
   Lehï¿½r, Franz                    | "\"Lehï¿½r, Franz\""
   Le Roux, Gaspard                  | "\"Le Roux, Gaspard\""
   Sterndale Bennett, William        | "\"Sterndale Bennett, William\""
   Strauss II, Johann                | "\"Strauss II, Johann\""
   Strauss, Johann, Sr               | "\"Strauss, Johann, Sr\""
   Suppï¿½, Franz von                | "\"Suppï¿½, Franz von\""

**conductor names to normalize (basic)**
::

                conductor                | plays 
  ---------------------------------------+-------
   Tullio Serafin, conductor             |     3
   Robert Shaw, conductor                |     2
   Bernard Haitink, cond.                |     1
   Bryden Thomson, conductor             |     1
   Eckart Hübner, bassoon and conductor  |     1
   "James DePriest, conductor"           |     1
   Neeme Järvi, cond.                    |     1
   Pierre Boulez, conductor              |     1
   Robert Shaw, cond.                    |     1
   Trevor Pinnock, cond.                 |     1
   Victoria Bond, conductor              |     1
   Evgueni Bushkov, violin and leader    |     1
   Robert Salter, leader (concertmaster) |     1

**ensemble names to normalize (basic)**
::

               ensemble             | plays 
  ----------------------------------+-------
   Andre Previn, piano              |     1
   Andrew Manze, violin             |     1
   Angela Meade, soprano            |     1
   Barbara Westphal, viola          |     1
   Christian Ruvolo, piano          |     1
   Elizabeth DiFelice, piano        |     1
   Ian Buckle, piano                |     1
   Itzhak Perlman, violin           |     1
   Jamie Barton, mezzo-soprano      |     1
   Jonathan Aasgaard, cello         |     1
   Katherine Fink, flute            |     1
   Phillip Moll, harpsichord        |     1
   Richard Egarr, harpsichord       |     1
   Sarah Cunningham, viola da gamba |     1

**performer names to parse (intermediate-level?)**
::

  ens parseable
  -------------
  English Concert
  Boston Pops
  Bournemouth Symphony
  Chamber Orchestra of Europe
  Cleveland O.
  Orpheus Chamber Orchestra
  I Musici
  I Solisti Italiani
  Albert Schweitzer Quintet
  Athena Ensemble
  BBC Philharmonic
  Beaux Arts Trio
  Buffalo Philharmonic
  Boston Pops O.
  Chorus and Symphony of Montreal
  Cincinnati Pops Orchestra
  Cleveland Orchestra
  Czech Philharmonic
  Dresden State Orchestra
  English Chamber Orchestra
  English C.O.
  English Sym. Orch.
  Guildhall String Ens.
  Israel Philharmonic
  Israel P.O./Mehta
  Juilliard Quartet
  La Fontegara
  La Serenissima
  Jean-Francois  Paillard Chamber Orchestra
  London Mozart Players
  London Philharmonic Orchestra
  Mahler Chamber Orchestra
  MDR Leipzig Radio Symphony Orches
  Montreal S.O.
  Montreal Symphony Orchestra
  Musicians of the Old Post Road
  New York Philharmonic
  Orchestra of St. Luke's
  Orchestra of the Swiss Romande
  Philadelphia Orchestra
  Royal P.O.
  San Francisco Symphony Orchestra
  St. Paul Chamber Orchestra
  St. Paul C.O.

  ens/cond parsable
  -----------------
  Academy of St. Martin-in-the-Fields/Marriner
  Boston Symphony Orchestra/Haitink
  Cincinnati Pops O./Kunzel
  O. of the 18th Century/Brunelle
  Orch. of the Nat'l. Acad. of St. Cecilia/Chung
  RCA S.O./Steinberg
  St. Paul C.O./Zukerman
  The Philharmonia
  The Silk Road Ensemble
  Ulster Orchestra
  Wiener Johann Strauss Orchestra

  performer/role parsable
  -----------------------
  Shaham, Gil, vl.
  Arrau, Claudio, pf.
  De Larrocha, Alicia, pf.
  Goode, Richard, pf.
  Hammes, Thomas, trpt
  Helmrich, Dennis, pf.
  Kocsis, Zoltan, pf.
  Marsalis, Branford, sax.
  Mayorga, Lincoln, pf.
  O'Conor, John, pf.
  Perlman, Itzhak, vl.
  Sanders, Samuel, pf.
  Schocker, Gary, fl.
  Tharaud, Alexandre, pf.

  normal/recognizable names
  -------------------------
  Andre Previn
  Itzhak Perlman
  Christopher Parkening
  Leif Ove Andsnes
  Pahud, Emmanuel,
  Renee Fleming
  Renée Fleming
  Richard Dowling
  Simone Dinnerstein
  Tanglewood Festival
  Yo-Yo Ma

  library names
  -------------
  Academy of St. Martin-in-the-Fields
  ASMF
  A.S.M.F.

  others (hard to parse)
  ----------------------
  Duo Tal & Groethuysen
  Angele Dubeau & La Pieta
  Boston Cello Quartet: Dejardin
  Br
  Chandler
  Chaplin
  Empire Brass Quintet & Friends
  ensemble
  Esbensen
  Fez
  Green
  Green, cello, Nancy
  Hennessy
  Jarvi
  Jojatu
  Joshua Bell: Home with friends
  Lecarme
  Moyer
  Parkening
  Perahi
  Whelen

**abstract entity string parsing**

* enclosing matched delimiters (quotes, parens, braces, etc.), entire string ("entity string")
* enclosing matched delimiters, substring ("entity item")
* incomplete matching delimiters (not terminated), for entire string (and items???)
* leading delimiters, for entire string and items
* item-separating delimiters
   * identify and track position
   * parse out individual fields
   * classification (NER) of fields
   * assemble entity items based on:
      * delimiter hierarchy/significance
      * logical field groupings

**entity string table**

* entity_string
* source (category: program, composer, conductor, ensemble, performer, work)
* parsed data (jsonb)
* station_id (denorm)
* program_play_id
* play_id

**entity table**

* entity_name
* entity_type (first name, last name, program, host, composer, ensemble, role, <hybrid>[?], etc.)

**entity groups (sequences)**

* entity_type, entity_type, ... = uber-entity_type (delimiters abstracted out)

------------------------
To Do - Immediate/Active
------------------------

* basic normalization for conductor, performer, and ensemble
* rationalize use of "entity" (as either relational table or name/proper noun)!!!
* investigate anomalies with play_seq matches
* rectify program based on play_seq matches
* debug/fix work/play with composer/person '<none>'
* rectify denorm of composer/conductor/ensemble in performer/play_performer
* identify syndicated plays, factor out of queries (using master_play_id)
* add stations: WQXR, WFMT, KUSC, WDAV, KING, WETA, KDFC, KQAC
* debug/fix outstanding anomalies for person
* robustify play_seq (program-/hour-boundaries, carry-over between playlists, etc.)
* add UTC start_time/end_time for program_play and play
* play_seq_match analysis (utilizing UTC?)

-------------------
To Do - Bugs/Tweaks
-------------------

* play_start AM/PM not parsed right for C24 and MPR
* figure out duplicate start time (different works) for plays (WWFM)
* make sure unicode handling in names is correct (even before canonicalization)
* more normalization for person
* add ``--force`` flag to overwrite existing playlists
* force-pull all stations 09/13-09/18 due to previous tight (15 minute) cron window
* create backup files for ``station_info.json`` and ``playlists.json``

----------------
To Do - Features
----------------

* consolidated tracking for all entity names (preparation for NER)--"entity cloud"
* authoritative musiclib/ref data (e.g. from archivmusic)
* track **all** data fixups (whether manual or programmatic) so that they are re-applyable!!!
* **music module integrity**
* fork/port to python3 (rename to cmir)--should do as soon as tests are in place!!!
* make logging (and printing for CLI commands) consistent
* write valid, missing, invalid to state structure
* fetch missing playlists
* validate playlist contents, record as metadata
* context manager for throttling playlist fetches
* job queue for playlist fetches (cycle through stations)
* get older playlists (determine epoch/beginning of time) for all stations
* archive function for playlists (and station info)
* locate ``stations`` directory in ``config.yml`` (can be outside of cmir)

-----------
Investigate
-----------

**hash_seq matches < 20 (hash_level = 1)**
::

   synd_level | name | hash_level | synd_level | name | count
  ------------+------+------------+------------+------+-------
          100 | C24  |          1 |         50 | IPR  |   558
          100 | C24  |          1 |         90 | MPR  |   533
          100 | C24  |          1 |         70 | WWFM |   493
          100 | C24  |          1 |         60 | WMHT |   365
          100 | C24  |          1 |         80 | VPR  |   365
          100 | C24  |          1 |         40 | WIAA |   244
          100 | C24  |          1 |         30 | WCRB |    17
          100 | C24  |          1 |        100 | C24  |    10
          100 | C24  |          1 |         20 | WNED |     8
          100 | C24  |          1 |         10 | WRTI |     4
  (10 rows)

**change in order (from previous) for hash_level = 2**
::

   synd_level | name | hash_level | synd_level | name | count
  ------------+------+------------+------------+------+-------
          100 | C24  |          2 |         50 | IPR  |   516
          100 | C24  |          2 |         70 | WWFM |   462
          100 | C24  |          2 |         90 | MPR  |   447
          100 | C24  |          2 |         60 | WMHT |   330
          100 | C24  |          2 |         80 | VPR  |   327
          100 | C24  |          2 |         40 | WIAA |   221
  (6 rows)

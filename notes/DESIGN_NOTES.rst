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

**crontab entry**
::

  15 0 * * * /local/prod/cmprod/scripts/cm_station.sh --fetch --date=catchup --debug=1 all >> /local/prod/cmprod/log/cm_station.log 2>&1

-------------------
To Do - Bugs/Tweaks
-------------------

* add ``--force`` flag to overwrite existing playlists
* get rid of ``playlists`` from ``station_info.json`` file
* create backup files for ``station_info.json`` and ``playlists.json``

----------------
To Do - Features
----------------

* create playlists module
* create music module
* make logging (and printing for CLI commands) consistent
* write valid, missing, invalid to state structure
* fetch missing playlists
* validate playlist contents, record as metadata
* decorator for throttling playlist fetches
* job queue for playlist fetches (cycle through stations)
* get older playlists (to beginning of time) for all stations
* archive function for playlists (and station info)
* create database schema
* Fork/port to python3 (rename to cmir)
* locate ``stations`` directory in ``config.yml`` (can be outside of cmir)

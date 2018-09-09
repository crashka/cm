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
    info:      {...},
    state:     {...},
    playlists: {...},
    shows:     {... }
  }

--------------
playlists.json
--------------

::

  {
    <%Y-%m-%d>: {
      file:   <filename>,
      status: <status value>
    },
    <%Y-%m-%d>: {
      ...
    },
    ...
  }

---------------
Timezone values
---------------

* ``America/New_York``
* ``America/Chicago``
* ``America/Denver``
* ``America/Los_Angeles``

-------------
Status values
-------------

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

----------------
To Do - Features
----------------

* fetch missing playlists
* catch up on playlists
* validate playlist contents
* rebuild metadata based on contents

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
    name:     <name>,
    timezone: <timezone value>,
    info:     {...},
    shows:    [<show 1>, <show 2>, ... ]
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

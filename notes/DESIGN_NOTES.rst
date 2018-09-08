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
    timezone: <timezone value>,
    shows: [<show 1>, <show 2>, ... ]
  }

--------------
playlists.json
--------------

::

  [
    {
      date: <%Y-%M-%d>,
      status: <status value>
    },
    {
      ...
    }
  ]

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

## WQXR Parsing Notes ##

The JSON file contains an object with with single **`"events"`** key, whose value is array
of program objects.

```json
  {
    "events": [
      {...},
      {...},
        .
        .
        .
    ]
  }
```

Each of the program objects under **`"events"`** looks like this:

```json
  {
    "id"                 : "event_1200AM",
    "show_id"            : 1125633,
    "object_id"          : 1125633,
    "event_title"        : "New York At Night with Lauren Rico",

    "iso_start_timestamp": "2022-11-15T05:00:00+00:00",
    "iso_end_timestamp"  : "2022-11-15T10:30:00+00:00",

    "start_timestamp"    : "2022-11-15T00:00:00",
    "end_timestamp"      : "2022-11-15T05:30:00",

    "time"               : "12:00 AM",
    "starttime"          : "12:00",
    "endtime"            : "05:30 AM",

    "playlists"          : [ {...} ]
  }
```

**`"playlists"`** is an array with a single object entry, whose structure is as follows:

```json
  {
    "id"    : "playlist_109557",
    "played": [
      {...},
      {...},
        .
        .
        .
    ]
  }
```

**`"played"`** is an array with an object entry for each piece played for the program.  The
structure of each (`play`) object is as follows:

```json
  {
    "iso_start_time": "2022-12-08T18:02:30+00:00",
    "info"          : "<div class=\"piece-info\"> ... </div>",
    "id"            : "entry_2493151",
    "time"          : "01:02 PM"
  }
```

The structure of the HTML under **`"info"`** looks like this:

```html
  <div class=\"piece-info\">
    <ul>
      <li>
        <a href=\"/music/musicians/teresa-carreno_/\"
          class=\"playlist-item__composer\">
          Teresa Carreno
        </a>
      </li>

      <li class=\"playlist-item__title\">Vals gayo</li>

      <li class=\"playlist-item__musicians\">
        <a href=\"/music/musicians/clara-rodriguez/\">Clara Rodriguez</a>, piano
      </li>
    </ul>

    <div class=\"album-info\">
      <ul class=\"playlist-actions\">
        <li class=\"playlist-item__album\">Album: Carreno | Piano Music</li>
      </ul>
    </div>
  </div>

  <div class=\"playlist-item__duration\">
    4:47
  </div>
```

Note that the class name is under the `<a>` tag for the composer field, whereas it is
under the `<li>` tag for the work title, performer(s), and album title fields.

Here is a more involved example of **`"info"`**:

```html
  <div class=\"piece-info\">
    <ul>
      <li>
        <a href=\"/music/musicians/wolfgang-amadeus-mozart/\"
          class=\"playlist-item__composer\">
          Wolfgang Amadeus Mozart
        </a>
      </li>

      <li class=\"playlist-item__title\">Requiem Mass in D Minor, K. 626</li>

      <li class=\"playlist-item__musicians\">
        <a href=\"/music/musicians/christine-brewer/\">Christine Brewer</a>, soprano
      </li>
      <li class=\"playlist-item__musicians\">
        <a href=\"/music/musicians/ruxandra-donose/\">Ruxandra Donose</a>, mezzo-soprano
      </li>
      <li class=\"playlist-item__musicians\">
        <a href=\"/music/musicians/john-tessier/\">John Tessier</a>, tenor
      </li>
      <li class=\"playlist-item__musicians\">
        <a href=\"/music/musicians/eric-owens/\">Eric Owens</a>, bass
      </li>

      <li class=\"playlist-item__musicians\">
        <a href=\"/music/ensembles/atlanta-symphony-orchestra/\">Atlanta Symphony Orchestra</a>
      </li>
      <li class=\"playlist-item__musicians\">
        <a href=\"/music/ensembles/atlanta-chamber-chorus/\">Atlanta Chamber Chorus</a>
      </li>

      <li class=\"playlist-item__musicians\">
        <a href=\"/music/musicians/donald-runnicles/\">Donald Runnicles</a>, conductor
      </li>
    </ul>

    <div class=\"album-info\">
      <ul class=\"playlist-actions\">
        <li class=\"playlist-item__album\">Album: Mozart: Requiem</li>
      </ul>
    </div>
  </div>

  <div class=\"playlist-item__duration\">
    46:43
  </div>
```

Older example (from 2015-10-05):

```html
  <div class=\"piece-info\">
    <ul>
      <li>
        <a href=\"/music/musicians/vincenzo-bellini/\"
           class=\"playlist-item__composer\">
          Vincenzo Bellini
        </a>
      </li>
      <li class=\"playlist-item__title\">Sinfonia Breve in D Major</li>
      <li class=\"playlist-item__musicians\">
        <a href=\"/music/ensembles/chopin-chamber-orchestra/\">Chopin Chamber Orchestra</a>
      </li>
      <li class=\"playlist-item__musicians\">
        <a href=\"/music/musicians/winston-dan-vogel/\">Winston Dan Vogel</a>, conductor
      </li>
      <li>
        7 min 7 s
      </li>
    </ul>
  </div>
  <div class=\"album-info\">
    <ul class=\"playlist-actions\">
      <li class=\"playlist-buy\">
        <a target=\"_blank\" href=\"http://www.arkivmusic.com/classical/Playlist?source=WQXR&cat=72&id=127424&label=Dynamic\">Buy Track</a>
      </li>
    </ul>
  </div>
```

----
WWFM
----

**code:**
::

  pl_params = playlist['params']

  pl_progs = playlist['onToday']
  for prog in pl_progs:
    prog_info = prog.get('program')
    for play in prog.get('playlist'):
      assert(type(play) == dict)

**json structures:**
::

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

---
MPR
---

**html:**
::

  <dl data-playlist-service-base="/playlist/classical-mpr" id="playlist">
    <dt>
      <h2>
        11:00 PM – 12:00 AM
      </h2>
    </dt>
    <dd>
      <ul>
        <li id="song349393">
          <a class="button small buy-button" href="http://www.arkivmusic.com/..." title="Purchase...">
            Buy
          </a>
          <a class="song-time" data-pjax="true" href="https://www.classicalmpr.org/playlist/...">
            <time datetime="2018-09-26">
              11:44
            </time>
          </a>
          <div class="song-info">
            <h3 class="song-title">
              Supplica
            </h3>
            <h4 class="song-composer">
              Christopher Rouse
            </h4>
            <h4 class="song-conductor">
              Carlos Kalmar
            </h4>
            <h4 class="song-orch_ensemble">
              Oregon Symphony
            </h4>
            <h4 class="song-soloist soloist-1">
              Francisco Fullana, violin
            </h4>
          </div>
        </li>
      </ul>
    </dd>
  </dl>

----
WQXR
----

**notes:**

* "events" = programs (in forward order)
   * only one "playlist" per "event"???  (need to validate)
* "played" items = plays (within playlist), in **reverse** order

**json document (with html elements for program/play info):**
::

  {
    "events": [
      {
        "current": "",
        "end_timestamp": "2018-09-17T05:30:00",
        "endtime": "05:30 AM",
        "evenOdd": "odd",
        "event_title": "New York At Night",
        "event_url": "http://www.wqxr.org/shows/overnight-music",
        "id": "event_1200AM",
        "isEpisode": false,
        "isObject": true,
        "object_id": 316,
        "playlists": [
          {
            "comment_count": 0,
            "has_comments": false,
            "id": "playlist_70079",
            "played": [
              {
                "id": "entry_1595540",
                "info": <playlist item>,
                "time": "05:24 AM"
              },
                .
                .
                .
            ],
            "url": "http://www.wqxr.org/music/playlists/show/overnight-music/2018/sep/17/"
          }
        ],
        "scheduletease": <schedule tease>,
        "scheduleteasehead": <schedule tease head>,
        "show_id": 316,
        "show_title": "New York At Night",
        "show_url": "http://www.wqxr.org/shows/overnight-music",
        "start_timestamp": "2018-09-17T00:00:00",
        "starttime": "12:00",
        "time": "12:00 AM",
        "top_commentcount": 0,
        "top_playlisturl": "http://www.wqxr.org/music/playlists/show/overnight-music/2018/sep/17/"
      },
    ]
  }

**schedule tease head html:**
::

  <div class=\"program\">
    <a href=\"http://www.wqxr.org/shows/overnight-music\">New York At Night</a>
  </div>
  <div class=\"expand\">
    <div class=\"arrow\"></div>
  </div>
  <div class=\"options\">
    <div></div>
  </div>

**schedule tease html:**
::

  <div class=\"program clearfix\">
    <div class=\"image\">
      <a href=\"http://www.wqxr.org/shows/overnight-music\"> <img src=\"https://media.wnyc.org/i/60/60/l/80/1/NewYorkAtNight_WQXR_ShowPageSquares.png\" />
      </a> </div>
    <div class=\"text\">
      <div class=\"tease\"><div class=\"no-object\">
        <p>Tune in for a nightly mix that spans the centuries.</p>
      </div></div>
      <ul class=\"hosts\">
        <li>Host: </li>
        <li><a href=\"/people/nimet-habachy/\">Nimet Habachy</a></li>
      </ul>
      <div class=\"scheduled-item-link\">
        Go to program: <a href=\"http://www.wqxr.org/shows/overnight-music\">New York At Night</a>
      </div>
      <div class=\"expand\"></div>
    </div>
  </div>

**playlist item ("played") html:**
::

  <div class="piece-info">
    <ul>
      <li>
        <a class="playlist-item__composer" href="/music/musicians/frederick-delius/">
          Frederick Delius
        </a>
      </li>
      <li class="playlist-item__title">On Hearing the First Cuckoo in Spring</li>
      <li class="playlist-item__musicians">
        <a href="/music/ensembles/the-halle-orchestra/">The Halle Orchestra</a>
      </li>
      <li class="playlist-item__musicians">
        <a href="/music/musicians/mark-elder/">Mark Elder</a>, conductor
      </li>
      <li>
        6 min 2 s
      </li>
    </ul>
  </div>

  <div class="album-info">
    <ul class="playlist-actions">
      <li class="playlist-buy">
        <a href="http://www.arkivmusic.com/classical/Playlist?source=WQXR&amp;cat=7512&amp;id=127171&amp;label=CD+Hill" target="_blank">Buy Track</a>
      </li>
    </ul>
  </div>

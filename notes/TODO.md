
## To Do - New ##

- opportunistically add docstrings and type annotations
- create classes for `ProgPlay`, `Play`, and `EntityStrData`
- `MusicEnt` should have its own module
- create `EntityItem` and subclasses (composer, work, conductor, performer, ensemble)
- push performer/role parsing upstream into `map_play`, where/when possible
- use refdata to aid in parsing of people and ensemble names
- MusicLib should not be a class, convert methods to top-level functions
- identify truncated entries (e.g. performer), and make parsable
- break playlist parsers into separate files/submodules
- merge definitive fetched playlists (caladan and arrakis)
- new module to normalize names of works
- addition refdata sources
- refdata for composer-works???
- ETL to analytics warehouse???

## To Do - Immediate/Active ##

- filter out NULL characters ('\u0000') from playlists on fetch (log as warning[?])
    - fixup of existing files with NULL characters (WXXI)
    - also continue to detect in playlist parsing???
- merge split plays (across date/hour boundaries), log fixups
- abstract entity_string parsing
    - map "replacement character" (\ufffd for WRTI; \xef\xbf\xbd for WMHT) to wildcard for matching
- rationalize use of "entity" (as either relational table or name/proper noun)!!!
- investigate anomalies with play_seq matches (interval > 5 minutes)
- robustify play_seq (program-/hour-boundaries, carry-over between playlists, etc.)
- rectify program based on play_seq matches
- debug/fix work/play with composer/person '<none>' and work '<unknown>'
- identify syndicated plays, factor out of queries (using master_play_id)
- add stations: WQXR, WFMT, KUSC, WDAV, KING, WETA, KDFC, KQAC
- debug/fix outstanding anomalies for person
- play\_seq\_match analysis/algorithm tuning (utilizing UTC!)

## To Do - Bugs/Tweaks ##

- handle HTTP status code correctly
- figure out duplicate start time (different works) for plays (WWFM)
- more normalization for person (if needed, after abstract entity parsing)
- create backup files for ``station_info.json`` and ``playlists.json``

## To Do - Features ##

- consolidated tracking for all entity names (preparation for NER)--"entity cloud"
- authoritative musiclib/ref data (e.g. from arkivmusic)
- jupyter notebooks for data quality, and for play analysis
- track **all** data fixups (whether manual or programmatic) so that they are re-applyable!!!
- **music module integrity**
- make logging (and printing for CLI commands) consistent
- write valid, missing, invalid to state structure
- fetch missing playlists
- validate playlist contents, record as metadata
- context manager for throttling playlist fetches
- job queue for playlist fetches (cycle through stations)
- automatically get older playlists (determine epoch) for all stations
- archive function for playlists (and station info)
- locate ``stations`` directory in ``config.yml`` (can be outside of cmir)

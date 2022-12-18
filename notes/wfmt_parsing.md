## WFMT Parsing Notes ##

Narrow context to the wrapper div for the playlist:

```html
  <div class="entry-wrap">
```

Playlist date can be parsed out of this:

```html
  <h6 class="mtn" id="schedule-title">
    Playlist for Wednesday, October 19, 2022
  </h6>
```

Narrow context to playlist content div:

```html
  <div id="playlist-main">
```

Programs look like this (where each piece played is contained in `<div class="item clearfix">`):

```html
  <div class="content-block">
    <h2 class="time-block">
      6:00 - 10:00
      <span style="font-size: smaller">
        am
      </span>
    </h2>
    <div class="program-info clearfix">
      <p class="program-time">
        6:00 am
      </p>
      <h3 class="program-title">
        <a href="https://www.wfmt.com/programs/mornings-with-dennis-moore/" target="">
          Mornings with Dennis Moore
        </a>
      </h3>
      <p class="program-desc">
        Including news &amp; weather on the hour between 6:00 am and 9:00 am; and "Carl’s Almanac" at 7:30 am.
      </p>
    </div>
    <div class="item clearfix"></div>
    <div class="item clearfix"></div>
    <div class="item clearfix"></div>
    <div class="item clearfix"></div>
            .
            .
            .
  </div>
```

Sample play structures:

```html
  <div class="item clearfix">
    <div class="time-played">
      <p>
        8:19 am
      </p>
    </div>
    <div class="item-info">
      <h4 class="composer-title">
        <span class="composer">
          Manuel Ponce
        </span>
        :
        <span class="title">
          "Concierto del Sur"
        </span>
      </h4>
      <p class="orchestra-conductor">
        Finale, Allegro moderato e festivo
      </p>
      <p class="soloists">
        Pablo Sáinz Villegas, g; Phil Orch of the Americas/Alondra de la Parra
        <br/>
        Mi Alma Mexicana * My Mexican Soul
      </p>
      <p class="album-meta">
        Sony 88697755552
      </p>
    </div>
  </div>
```

```html
  <div class="item clearfix">
    <div class="time-played">
      <p>
        10:27 am
      </p>
    </div>
    <div class="item-info">
      <h4 class="composer-title">
        <span class="composer">
          Louise Farrenc
        </span>
        :
        <span class="title">
          Clarinet Trio in E-Flat, Op. 44
        </span>
      </h4>
      <p class="orchestra-conductor">
        IV. Finale. Allegro
      </p>
      <p class="soloists">
        Romain Guyot, cl; François Salque, vc; Brigitte Engerer, p
        <br/>
        Louise Farrenc - Chamber Music
      </p>
      <p class="album-meta">
        Naïve V-5033
      </p>
    </div>
  </div>
```

**Exception/special case** formatting for program "Through the Night with Peter van de Graaff":

```html
  <div class="item clearfix">
    <div class="time-played">
      <p>
        12:35 am
      </p>
    </div>
    <div class="item-info">
      <h4 class="composer-title">
        <span class="composer">
          Copland, Aaron
        </span>
        :
        <span class="title">
          Clarinet Concerto
        </span>
      </h4>
      <p class="orchestra-conductor">
        London Symphony Orchestra / Gregor Bühl, conductor
      </p>
      <p class="soloists">
        Sharon Kam, clarinet,
      </p>
      <p class="album-meta">
        Teldec 84482-2
      </p>
    </div>
  </div>
```

```html
  <div class="item clearfix">
    <div class="time-played">
      <p>
        4:26 am
      </p>
    </div>
    <div class="item-info">
      <h4 class="composer-title">
        <span class="composer">
          Chopin, Frédéric
        </span>
        :
        <span class="title">
          Preludes, Op.28
        </span>
      </h4>
      <p class="soloists">
        Andrea Lucchesini, piano,p,
      </p>
      <p class="album-meta">
        EMI/Ang CDC7-49725-2
      </p>
    </div>
  </div>
```

Notes for exception/special case:

- This is actually a more accurate use of the div tags, but not clear how
movements/fragments are represented (have not yet run across a good example).
- It will take some figuring out how to do fixup on the performer information and some of
  the title information, as it appears to be consistent sloppy

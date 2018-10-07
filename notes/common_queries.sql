
-- partial index on recording (alt key)
create unique index recording_altkey
    on recording (name, label)
 where catalog_no is null;

-- plays by composer
select substr(p.name, 1, 60) as composer, count(*) as plays
  from person p
       join play pl on pl.composer_id = p.id
 group by 1
 order by 2 desc, 1;

-- plays by conductor
select substr(p.name, 1, 60) as conductor, count(*) as plays
  from person p
       join play pl on pl.conductor_id = p.id
 group by 1
 order by 2 desc, 1;

select p.name, count(*) as plays, array_agg(distinct s.name), array_agg(pl.id)
  from person p
       join play pl on pl.conductor_id = p.id
       join station s on s.id = pl.station_id
 where p.name !~ '^[\w-]+( [\w-]+)+$'
 group by 1
 order by 1;

-- plays by ensemble
select substr(e.name, 1, 70) as ensemble, count(*) as plays
  from ensemble e
       join play_ensemble ple on ple.ensemble_id = e.id
       join play pl on pl.id = ple.play_id
 group by 1
 order by 2 desc, 1;

-- plays by performer
select substr(p.name, 1, 60) as performer, pf.role, count(*) as plays
  from performer pf
       join person p on p.id = pf.person_id
       join play_performer plf on plf.performer_id = pf.id
       join play pl on pl.id = plf.play_id
 group by 1, 2
 order by 3 desc, 1, 2;

-- works by composer
select substr(p.name, 1, 60) as composer, count(*) as works
  from person p join work w on w.composer_id = p.id
 group by 1
 order by 2 desc, 1;

-- distinct composers by station
select s.name as station, count(distinct p.name) as distinct_composers
  from station s
       join play pl on pl.station_id = s.id
       join person p on p.id = pl.composer_id
 group by 1
 order by 2 desc, 1;

-- play of works by composer '<none>'
select w.name, count(*)
  from work w
       join person p on p.id = w.composer_id and p.name = '<none>'
       join play pl on pl.work_id = w.id
 group by 1
 order by 2 desc, 1;

select w.name, count(*), array_agg(pl.id), array_agg(distinct s.name)
  from work w
       join person p on p.id = w.composer_id and p.name = '<none>'
       join play pl on pl.work_id = w.id
       join station s on s.id = pl.station_id
 group by 1
 order by 2 desc, 1;

select p.name, w.name, count(*), array_agg(pl.id), array_agg(distinct s.name)
  from work w
       join person p on p.id = w.composer_id and p.name = '<none>'
       join play pl on pl.work_id = w.id
       join station s on s.id = pl.station_id
 group by 1, 2
 order by 3 desc, 1, 2;

-- find syndicated plays (TODO: dedupe same play at different hash levels!!!)
select ps.seq_hash, ps.hash_level, count(*) - 1 as num_subs, max(s.synd_level) as master,
       array_remove(array_agg(s.synd_level order by s.synd_level desc), max(s.synd_level)) as subs
  from play_seq ps
       join play pl on pl.id = ps.play_id
       join station s on s.id = pl.station_id
 where ps.hash_level > 1
 group by 1, 2 having count(*) > 1
 order by 2 desc, 3 desc;

-- find syndication followers (TODO: dedupe same play at different hash levels!!!)
select s.synd_level, s.name, ps.hash_level, s2.synd_level, s2.name, count(pl2)
  from play pl
       join station s on s.id = pl.station_id
       join person cp on cp.id = pl.composer_id
       join work w on w.id = pl.work_id
       join play_seq ps on ps.play_id = pl.id
       join play_seq ps2 on ps2.seq_hash = ps.seq_hash and
                            ps2.hash_level = ps.hash_level and
                            ps2.id != ps.id
       join play pl2 on pl2.id = ps2.play_id
       join station s2 on s2.id = pl2.station_id
 where s.name = 'C24'
   and ps.hash_level > 1
 group by 1, 2, 3, 4, 5
 order by 3 desc, 6 desc;

-- TODOs:
--   * by time as well as count
--   * distinct from other stations
--   * syndicated vs. local programming/shows

-- investigate seq_hash = 0
select ps.hash_level, s.name as station, comp.name as composer, w.name as work,
       pl.play_date, pl.play_start
  from play_seq ps
       join play pl     on pl.id   = ps.play_id
       join person comp on comp.id = pl.composer_id
       join work w      on w.id    = pl.work_id
       join station s   on s.id    = pl.station_id
 where seq_hash = 0
 order by 1 desc, 3, 4, s.synd_level desc;

-- composers with non-standard names
select distinct p.name from person p
       join play pl on pl.composer_id = p.id
 where p.name !~ '^[\w-]+( [\w-]+)+$'
 order by 1;

select p.name, count(*) as plays, array_agg(distinct s.name), array_agg(pl.id)
  from person p
       join play pl on pl.composer_id = p.id
       join station s on s.id = pl.station_id
 where p.name !~ '^[\w-]+( [\w-]+)+$'
 group by 1
 order by 1;

select p.name, array_agg(pl.id)
  from person p
       join play pl on pl.composer_id = p.id
 where p.name !~ '^\w+ \w+$'
 group by 1
 order by 1;

select p.name, array_agg(pl.id)
  from person p
       join play pl on pl.composer_id = p.id
 where p.name !~ '^(\w+ ){1,2}\w+$'
 group by 1
 order by 1;

-- quoted
select distinct p.name
  from person p
       join play pl on pl.composer_id = p.id
 where p.name ~ '^"[^"]+"$'
 order by 1;

-- single comma
select distinct p.name
  from person p
       join play pl on pl.composer_id = p.id
 where p.name ~ '^[^,]+,[^,]+"$'
 order by 1;

-- Jr.
select distinct p.name
  from person p
       join play pl on pl.composer_id = p.id
 where p.name ~ '\mJr\.?\M'
 order by 1;

-- Sr.
select distinct p.name
  from person p
       join play pl on pl.composer_id = p.id
 where p.name ~ '\mSr\.?\M'

-- II/III
select distinct p.name
  from person p
       join play pl on pl.composer_id = p.id
 where p.name ~ '\mI{2,3}\M'

-- all together
select distinct p.name
  from person p
       join play pl on pl.composer_id = p.id
 where p.name ~ '\m(Jr\.?|Sr\.?|I{2,3})\M';

-- investigate plays
select pl.id as play_id, s.name as station, s.synd_level, pr.name as program, pl.play_date,
       pl.play_start, cp.name as composer, w.name as work
  from play pl
       join station s on s.id = pl.station_id
       join program_play pp on pp.id = pl.prog_play_id
       join program pr on pr.id = pp.program_id
       join person cp on cp.id = pl.composer_id
       join work w on w.id = pl.work_id
 where pl.id in (4127, 4642) order by 8, 3 desc;

-- duplicate start_time for plays
select s.name, play_date, play_start, array_agg(work_id) as work_id,
       array_agg(c.name || ' - ' || w.name) composer_work
  from play pl
       join station s on s.id = pl.station_id
       join work w on w.id = pl.work_id
       join person c on c.id = w.composer_id
 group by 1, 2, 3
having count(*) > 1
 order by 1, 2, 3;

--investigate performers with null role
select substr(p.name, 1, 60) as performer, pf.role, count(*) as plays,
       array_agg(pl.id), array_agg(distinct s.name)
  from performer pf
       join person p on p.id = pf.person_id
       join play_performer plf on plf.performer_id = pf.id
       join play pl on pl.id = plf.play_id
       join station s on s.id = pl.station_id
 where pf.role is null
 group by 1, 2
 order by 3 desc, 1, 2;

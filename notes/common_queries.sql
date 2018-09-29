
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

-- plays by performer
select substr(p.name, 1, 60) as performer, f.role, count(*) as plays
  from performer f
       join person p on p.id = f.person_id
       join play_performer plf on plf.performer_id = f.id
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

-- find syndicated plays
select ps.seq_hash, ps.hash_level, count(*), max(s.synd_level), min(s.synd_level)
  from play_seq ps
       join play pl on pl.id = ps.play_id
       join station s on s.id = pl.station_id
 group by 1, 2 having count(*) > 1
 order by 3 desc;

-- TODOs:
--   * by time as well as count
--   * distinct from other stations
--   * syndicated vs. local programming/shows

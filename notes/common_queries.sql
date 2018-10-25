
-- partial index on recording (alt key)
create unique index recording_altkey
    on recording (name, label)
 where catalog_no is null

-- plays by composer
select substr(p.name, 1, 60) as composer, count(*) as plays
  from person p
       join play pl on pl.composer_id = p.id
 where pl.mstr_play_id is null
 group by 1
 order by 2 desc, 1

-- plays by conductor
select substr(p.name, 1, 60) as conductor, count(*) as plays
  from person p
       join play pl on pl.conductor_id = p.id
 where pl.mstr_play_id is null
 group by 1
 order by 2 desc, 1

select p.name, count(*) as plays, array_agg(distinct s.name), array_agg(pl.id)
  from person p
       join play pl on pl.conductor_id = p.id
       join station s on s.id = pl.station_id
 where p.name !~ '^[\w-]+( [\w-]+)+$'
   and pl.mstr_play_id is null
 group by 1
 order by 1

-- plays by ensemble
select substr(e.name, 1, 70) as ensemble, count(*) as plays
  from ensemble e
       join play_ensemble ple on ple.ensemble_id = e.id
       join play pl on pl.id = ple.play_id
 where pl.mstr_play_id is null
 group by 1
 order by 2 desc, 1

-- plays by performer
select substr(p.name, 1, 60) as performer, pf.role, count(*) as plays
  from performer pf
       join person p on p.id = pf.person_id
       join play_performer plf on plf.performer_id = pf.id
       join play pl on pl.id = plf.play_id
 where pl.mstr_play_id is null
 group by 1, 2
 order by 3 desc, 1, 2

-- works by composer
select substr(p.name, 1, 60) as composer, count(*) as works
  from person p join work w on w.composer_id = p.id
 group by 1
 order by 2 desc, 1

-- distinct composers by station
select s.name as station, count(distinct p.name) as distinct_composers
  from station s
       join play pl on pl.station_id = s.id
       join person p on p.id = pl.composer_id
 group by 1
 order by 2 desc, 1

-- play of works by composer '<none>'
select w.name, count(*)
  from work w
       join person p on p.id = w.composer_id and p.name = '<none>'
       join play pl on pl.work_id = w.id
 where pl.mstr_play_id is null
 group by 1
 order by 2 desc, 1

select w.name, count(*), array_agg(pl.id), array_agg(distinct s.name)
  from work w
       join person p on p.id = w.composer_id and p.name = '<none>'
       join play pl on pl.work_id = w.id
       join station s on s.id = pl.station_id
 where pl.mstr_play_id is null
 group by 1
 order by 2 desc, 1

select p.name, w.name, count(*), array_agg(pl.id), array_agg(distinct s.name)
  from work w
       join person p on p.id = w.composer_id and p.name = '<none>'
       join play pl on pl.work_id = w.id
       join station s on s.id = pl.station_id
 where pl.mstr_play_id is null
 group by 1, 2
 order by 3 desc, 1, 2

-- find syndicated plays (TODO: dedupe same play at different hash levels!!!)
select ps.seq_hash, ps.hash_level, count(*) - 1 as num_subs, max(s.synd_level) as master,
       array_remove(array_agg(s.synd_level order by s.synd_level desc), max(s.synd_level)) as subs
  from play_seq ps
       join play pl on pl.id = ps.play_id
       join station s on s.id = pl.station_id
 where ps.hash_level > 1
 group by 1, 2 having count(*) > 1
 order by 2 desc, 3 desc

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
 order by 3 desc, 6 desc

-- find syndication masters by play_seq
with master_synd as (
select ps.seq_hash, max(s.synd_level) as max_synd_level
  from play_seq ps
       join play pl on pl.id = ps.play_id
       join station s on s.id = pl.station_id
 where ps.hash_level = 3
 group by ps.seq_hash
having count(*) > 1
)
select ps.seq_hash, min(pl.id) as min_play_id
  from play_seq ps
       join play pl on pl.id = ps.play_id
       join station s on s.id = pl.station_id
 where (ps.seq_hash, s.synd_level) in (select * from master_synd)
 group by ps.seq_hash

-- find master play_id for plays
select pl.id, s.synd_level, min(pl2.id)
  from play pl
       join play_seq ps on ps.play_id = pl.id
       join station s on s.id = pl.station_id
       join play_seq ps2 on ps2.seq_hash = ps.seq_hash and
                            ps2.hash_level = ps.hash_level and
                            ps2.hash_type = ps.hash_type and
                            ps2.play_id != pl.id
       join play pl2 on pl2.id = ps2.play_id
       join station s2 on s2.id = pl2.station_id
 where s2.synd_level = (
       select max(is2.synd_level)
         from play ipl
              join play_seq ips on ips.play_id = ipl.id
              join station is1 on is1.id = ipl.station_id
              join play_seq ips2 on ips2.seq_hash = ips.seq_hash and
                                    ips2.hash_level = ips.hash_level and
                                    ips2.hash_type = ips.hash_type and
                                    ips2.play_id != ipl.id
              join play ipl2 on ipl2.id = ips2.play_id
              join station is2 on is2.id = ipl2.station_id
        where ipl.id = pl.id
          and ips.hash_level = 3
          and is2.synd_level >= is1.synd_level
          and ips2.play_id != ips.play_id
       )
   and ps.hash_level = 3
 group by 1, 2
 order by 2 desc, 1 desc

-- update mstr_play_id
update play
   set mstr_play_id = (
select min(pl2.id)
  from play pl
       join play_seq ps on ps.play_id = pl.id
       join station s on s.id = pl.station_id
       join play_seq ps2 on ps2.seq_hash = ps.seq_hash and
                            ps2.hash_level = ps.hash_level and
                            ps2.hash_type = ps.hash_type and
                            ps2.play_id != pl.id
       join play pl2 on pl2.id = ps2.play_id
       join station s2 on s2.id = pl2.station_id
 where pl.id = play.id
   and ps.hash_level = 3
   and s2.synd_level = (
       select max(is2.synd_level)
         from play ipl
              join play_seq ips on ips.play_id = ipl.id
              join station is1 on is1.id = ipl.station_id
              join play_seq ips2 on ips2.seq_hash = ips.seq_hash and
                                    ips2.hash_level = ips.hash_level and
                                    ips2.hash_type = ips.hash_type and
                                    ips2.play_id != ipl.id
              join play ipl2 on ipl2.id = ips2.play_id
              join station is2 on is2.id = ipl2.station_id
        where ipl.id = pl.id
          and ips.hash_level = 3
          and is2.synd_level >= is1.synd_level
       )
)

update play
   set mstr_play_id = (
       select min(pl2.id)
         from play pl
              join play_seq ps on ps.play_id = pl.id
              join station s on s.id = pl.station_id
              join play_seq ps2 on ps2.seq_hash = ps.seq_hash and
                                   ps2.hash_level = ps.hash_level and
                                   ps2.hash_type = ps.hash_type and
                                   ps2.play_id != pl.id
              join play pl2 on pl2.id = ps2.play_id
              join station s2 on s2.id = pl2.station_id
        where pl.id = play.id
          and ps.hash_level = 3
          and s2.synd_level = (
              select max(is2.synd_level)
                from play ipl
                     join play_seq ips on ips.play_id = ipl.id
                     join station is1 on is1.id = ipl.station_id
                     join play_seq ips2 on ips2.seq_hash = ips.seq_hash and
                                           ips2.hash_level = ips.hash_level and
                                           ips2.hash_type = ips.hash_type and
                                           ips2.play_id != ipl.id
                     join play ipl2 on ipl2.id = ips2.play_id
                     join station is2 on is2.id = ipl2.station_id
               where ipl.id = pl.id
                 and ips.hash_level = 3
                 and is2.synd_level >= is1.synd_level
              )
       )
 where id in (
      select id from play ipl
                join play_seq ips on ips.play_id = ipl.id
                join station is1 on is1.id = ipl.station_id
                join play_seq ips2 on ips2.seq_hash = ips.seq_hash and
                                      ips2.hash_level = ips.hash_level and
                                      ips2.hash_type = ips.hash_type and
                                      ips2.play_id != ipl.id
                join play ipl2 on ipl2.id = ips2.play_id
                join station is2 on is2.id = ipl2.station_id
       where ipl.id = pl.id
         and ips.hash_level = 3
         and is2.synd_level > is1.synd_level
          or (is2.synd_level = is1.synd_level and
              ipl2.play_id < ipl.id)
     )

-- count subscribers by master_play_id
with master as (
select pl.id, s.synd_level, min(pl2.id) as master_id
  from play pl
       join play_seq ps on ps.play_id = pl.id
       join station s on s.id = pl.station_id
       join play_seq ps2 on ps2.seq_hash = ps.seq_hash and
                            ps2.hash_level = ps.hash_level and
                            ps2.hash_type = ps.hash_type and
                            ps2.play_id != pl.id
       join play pl2 on pl2.id = ps2.play_id
       join station s2 on s2.id = pl2.station_id
 where s2.synd_level = (
       select max(is2.synd_level)
         from play ipl
              join play_seq ips on ips.play_id = ipl.id
              join station is1 on is1.id = ipl.station_id
              join play_seq ips2 on ips2.seq_hash = ips.seq_hash and
                                    ips2.hash_level = ips.hash_level and
                                    ips2.hash_type = ips.hash_type and
                                    ips2.play_id != ipl.id
              join play ipl2 on ipl2.id = ips2.play_id
              join station is2 on is2.id = ipl2.station_id
        where ipl.id = pl.id
          and ips.hash_level = 3
          and is2.synd_level >= is1.synd_level
          and ips2.play_id != ips.play_id
       )
   and ps.hash_level = 3
 group by 1, 2
 order by 3, 1
)
select master_id, count(*), array_agg(synd_level order by synd_level desc)
  from master
 group by 1
 order by 2 desc, 1

-- TODOs:
--   * by time as well as count
--   * distinct from other stations
--   * syndicated vs. local programming/shows

-- composers with non-standard names
select distinct p.name from person p
       join play pl on pl.composer_id = p.id
 where p.name !~ '^[\w-]+( [\w-]+)+$'
 order by 1

select p.name, count(*) as plays, array_agg(distinct s.name), array_agg(pl.id)
  from person p
       join play pl on pl.composer_id = p.id
       join station s on s.id = pl.station_id
 where p.name !~ '^[\w-]+( [\w-]+)+$'
 group by 1
 order by 1

select p.name, array_agg(pl.id)
  from person p
       join play pl on pl.composer_id = p.id
 where p.name !~ '^\w+ \w+$'
 group by 1
 order by 1

select p.name, array_agg(pl.id)
  from person p
       join play pl on pl.composer_id = p.id
 where p.name !~ '^(\w+ ){1,2}\w+$'
 group by 1
 order by 1

-- single comma
select p.name, count(*), array_agg(distinct s.name order by s.name)
  from person p
       join play pl on pl.composer_id = p.id
       join station s on s.id = pl.station_id
 where p.name ~ '^[^,]+,[^,]+$'
 group by 1
 order by 1

-- Jr./Sr.
select p.name, count(*), array_agg(distinct s.name order by s.name)
  from person p
       join play pl on pl.composer_id = p.id
       join station s on s.id = pl.station_id
 where p.name ~ '\m(Jr|Sr)\.?\M'
 group by 1
 order by 1

-- II/III
select p.name, count(*), array_agg(distinct s.name order by s.name)
  from person p
       join play pl on pl.composer_id = p.id
       join station s on s.id = pl.station_id
 where p.name ~ '\mI{2,3}\M'
 group by 1
 order by 1

-- all together
select p.name, count(*), array_agg(distinct s.name order by s.name)
  from person p
       join play pl on pl.composer_id = p.id
       join station s on s.id = pl.station_id
 where p.name ~ '\m(Jr\.?|Sr\.?|I{2,3})\M'
 group by 1
 order by 1

-- investigate plays
select pl.id as play_id, s.name as station, s.synd_level, pr.name as program, pl.play_date,
       pl.play_start, cp.name as composer, w.name as work
  from play pl
       join station s on s.id = pl.station_id
       join program_play pp on pp.id = pl.prog_play_id
       join program pr on pr.id = pp.program_id
       join person cp on cp.id = pl.composer_id
       join work w on w.id = pl.work_id
 where pl.id in (4127, 4642) order by 8, 3 desc

-- duplicate start_time for plays
select s.name, play_date, play_start, array_agg(work_id) as work_id,
       array_agg(c.name || ' - ' || w.name) composer_work
  from play pl
       join station s on s.id = pl.station_id
       join work w on w.id = pl.work_id
       join person c on c.id = w.composer_id
 group by 1, 2, 3
having count(*) > 1
 order by 1, 2, 3

--investigate performers with null role
select substr(p.name, 1, 60) as performer, pf.role, count(*) as plays,
       array_agg(distinct s.name) as stations, array_agg(pl.id) as play_ids
  from performer pf
       join person p on p.id = pf.person_id
       join play_performer plf on plf.performer_id = pf.id
       join play pl on pl.id = plf.play_id
       join station s on s.id = pl.station_id
 where pf.role is null
 group by 1, 2
 order by 3 desc, 1, 2

-- compare station syndication, one day
select pl.start_time, s.name, p.name, substr(c.name, 1, 20), substr(w.name, 1, 25),
       ps1.seq_hash, ps2.seq_hash, ps3.seq_hash
  from play pl
       join station s on s.id = pl.station_id
       join program_play pp on pp.id = pl.prog_play_id
       join program p on p.id = pp.program_id
       join work w on w.id = pl.work_id
       join person c on c.id = w.composer_id
       join play_seq ps1 on ps1.play_id = pl.id and ps1.hash_level = 1
       left join play_seq ps2 on ps2.play_id = pl.id and ps2.hash_level = 2
       left join play_seq ps3 on ps3.play_id = pl.id and ps3.hash_level = 3
 where s.name in ('C24', 'WWFM', 'IPR', 'MPR', 'WWXI')
   and pl.start_time between '2018-10-15' and '2018-10-16'
 order by pl.start_time, s.synd_level desc

-- find syndicated plays, one day
select ps.seq_hash, substr(c.name, 1, 20), substr(w.name, 1, 25),
       max(s.synd_level) as s_lev, array_agg(distinct s.name) as stations,
       min(pl.start_time), max(pl.start_time), max(pl.start_time) - min(pl.start_time) as interval
  from play_seq ps
       join play pl on pl.id = ps.play_id
       join station s on s.id = pl.station_id
       join work w on w.id = pl.work_id
       join person c on c.id = w.composer_id
 where pl.start_time between '2018-10-15' and '2018-10-16'
   and ps.hash_level = 1
 group by 1, 2, 3
having count(*) > 1
 order by 6

-- find split plays (need to merge in code, and log the merges)
select pl1.start_time, pl1.end_time, pl2.end_time, s.name, p.name, substr(c.name, 1, 20), substr(w.name, 1, 25)
  from play pl1
       join play pl2 on pl2.station_id = pl1.station_id
                    and pl2.work_id = pl1.work_id
                    and pl2.start_time = pl1.end_time
       join station s on s.id = pl1.station_id
       join program_play pp on pp.id = pl1.prog_play_id
       join program p on p.id = pp.program_id
       join work w on w.id = pl1.work_id
       join person c on c.id = w.composer_id
 order by 1, s.synd_level desc

-- unknown composers by station and program
select s.name, p.name, count(*)
  from play pl
       join person c on  c.id = pl.composer_id
       join station s on s.id = pl.station_id
       join program_play pp on pp.id = pl.prog_play_id
       join program p on p.id = pp.program_id
 where not exists (select * from entity_ref where entity_ref = c.name)
 group by 1, 2
 order by 3 desc

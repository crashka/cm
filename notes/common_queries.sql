

-- plays by composer
select substr(p.name, 1, 60) as composer, count(*) as plays
  from person p
       join play pl on pl.composer_id = p.id
 group by 1 order by 2 desc, 1;

-- plays by conductor
select substr(p.name, 1, 60) as conductor, count(*) as plays
  from person p
       join play pl on pl.conductor_id = p.id
 group by 1 order by 2 desc, 1;

-- plays by performer
select substr(p.name, 1, 60) as performer, f.role, count(*) as plays
  from performer f
       join person p on p.id = f.person_id
       join play_performer plf on plf.performer_id = f.id
       join play pl on pl.id = plf.play_id
 group by 1, 2 order by 3 desc, 1, 2;

-- works by composer
select substr(p.name, 1, 60) as composer, count(*) as works
  from person p join work w on w.composer_id = p.id
 group by 1 order by 2 desc, 1;

-- distinct composers by station
select s.name as station, count(distinct p.name) as distinct_composers
  from station s
       join play pl on pl.station_id = s.id
       join person p on p.id = pl.composer_id
 group by 1 order by 2 desc, 1;

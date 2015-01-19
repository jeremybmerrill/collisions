DBNAME = "collisions"

all: collision_change_yoy.csv tables/per_precinct_collisions

NYPD_Motor_Vehicle_Collisions.csv: 
	wget -O $@ https://data.cityofnewyork.us/api/views/h9gi-nx95/rows.csv?accessType=DOWNLOAD

collisions.csv: NYPD_Motor_Vehicle_Collisions.csv
	echo "DATE,TIME,BOROUGH,ZIP_CODE,LATITUDE,LONGITUDE,LOCATION,ON_STREET_NAME,CROSS_STREET_NAME,OFF_STREET_NAME,NUMBER_OF_PERSONS_INJURED,NUMBER_OF_PERSONS_KILLED,NUMBER_OF_PEDESTRIANS_INJURED,NUMBER_OF_PEDESTRIANS_KILLED,NUMBER_OF_CYCLIST_INJURED,NUMBER_OF_CYCLIST_KILLED,NUMBER_OF_MOTORIST_INJURED,NUMBER_OF_MOTORIST_KILLED,CONTRIBUTING_FACTOR_VEHICLE_1,CONTRIBUTING_FACTOR_VEHICLE_2,CONTRIBUTING_FACTOR_VEHICLE_3,CONTRIBUTING_FACTOR_VEHICLE_4,CONTRIBUTING_FACTOR_VEHICLE_5,UNIQUE_KEY,VEHICLE_TYPE_CODE_1,VEHICLE_TYPE_CODE_2,VEHICLE_TYPE_CODE_3,VEHICLE_TYPE_CODE_4,VEHICLE_TYPE_CODE_5" > $@
	tail -n +2 $< >> $@

tables: 
	-createdb ${DBNAME} #- means don't abort on error
	-psql -d ${DBNAME} -c "CREATE EXTENSION postgis;"
	mkdir tables

tables/collisions: collisions.csv | tables
	cat createtable.sql | psql -d ${DBNAME}
	psql -d ${DBNAME} -c "\COPY ${DBNAME} FROM 'collisions.csv' WITH CSV HEADER"
	psql -d ${DBNAME} -c "ALTER TABLE collisions ADD COLUMN datetime timestamp;"
	psql -d ${DBNAME} -c "SELECT AddGeometryColumn ('collisions', 'geom', 4326, 'POINT', 2)";
	psql -d ${DBNAME} -c "UPDATE collisions SET datetime = (date || ' ' || time)::timestamp;"
	psql -d ${DBNAME} -c "UPDATE collisions SET geom = ST_PointFromText('POINT(' || longitude || ' ' || latitude ||')', 4326);"
	touch $@


nypp_13c_av/nypp.shp:
	wget -O nypp_13c_av.zip --no-check-certificate https://data.ny.gov/api/assets/A8BB7FF2-BDCD-417B-B183-01F8AB10FF89?download=true
	unzip nypp_13c_av.zip
	rm nypp_13c_av.zip
	touch nypp_13c_av


tables/nypp2263: nypp_13c_av/nypp.shp | tables
	psql -d ${DBNAME} -c "DROP TABLE IF EXISTS nypp2263"
	shp2pgsql -s 2263 nypp_13c_av/nypp.shp nypp2263 | psql -d ${DBNAME}
	touch $@ 

tables/nypp2263buffered: tables/nypp2263
	psql -d ${DBNAME} -c "$$BUFFERPRECINCTS"
	touch $@ 

tables/nypp: tables/nypp2263
	psql -d ${DBNAME} -c "DROP TABLE IF EXISTS nypp"
	psql -d ${DBNAME} -c "CREATE TABLE nypp AS SELECT *, ST_SetSRID(ST_Transform(geom, 4326), 4326) as the_geom from nypp2263;"
	psql -d ${DBNAME} -c "ALTER TABLE nypp DROP COLUMN geom;"
	psql -d ${DBNAME} -c "ALTER TABLE nypp RENAME COLUMN the_geom TO geom;"
	touch $@

tables/nyppbuffered: tables/nypp2263buffered
	psql -d ${DBNAME} -c "DROP TABLE IF EXISTS nyppbuffered"
	psql -d ${DBNAME} -c "CREATE TABLE nyppbuffered AS SELECT *, ST_SetSRID(ST_Transform(geom, 4326), 4326) as the_geom from nypp2263buffered;"
	psql -d ${DBNAME} -c "ALTER TABLE nyppbuffered DROP COLUMN geom;"
	psql -d ${DBNAME} -c "ALTER TABLE nyppbuffered RENAME COLUMN the_geom TO geom;"
	touch $@



analysis: table/collisions
	echo psql -d ${DBNAME} -c "$${ANALYSISQUERY}"


tables/per_precinct_collisions: tables/nyppbuffered tables/collisions
	psql -d ${DBNAME} -c "DROP TABLE IF EXISTS per_precinct_collisions"
	psql -d ${DBNAME} -c "$$YOYPERPRECINCTQUERY"
	touch tables/per_precinct_collisions

per_buffered_precinct_collisions.csv: tables/per_precinct_collisions
	psql -d ${DBNAME} -c "\copy (select * from per_precinct_collisions) to 'per_buffered_precinct_collisions.csv' with csv header;"

collision_change_yoy.csv: tables/collisions
	psql -d ${DBNAME} -c "$$YOYCHANGEQUERYFORCHART"

define ANALYSISQUERY
	select extract(month from datetime) AS month, extract(year from datetime) AS year, 
	sum(CASE WHEN NUMBER_OF_PERSONS_INJURED = 0 and NUMBER_OF_PERSONS_INJURED = 0 THEN 1 ELSE 0 END) AS minor,
	sum(CASE WHEN NUMBER_OF_PERSONS_INJURED > 0 or NUMBER_OF_PERSONS_INJURED > 0 THEN 1 ELSE 0 END) AS serious,
	count(*) AS incidents
	from collisions 
		where extract(month from datetime) = '12' and extract(year from datetime) in ('2014', '2013', '2012') and extract(day from datetime)::integer >= 22
		or extract(month from datetime) = '01' and extract(year from datetime) in ('2014', '2013', '2015', '2012') and extract(day from datetime)::integer <= 6
		group by extract(year from datetime), extract(month from datetime)
		order by month, year;
endef
export ANALYSISQUERY

define DECANALYSISQUERY
	select extract(month from datetime) AS month, extract(day from datetime) AS day,
	sum(NUMBER_OF_PERSONS_INJURED) AS injured, sum(NUMBER_OF_PERSONS_KILLED) AS killed, 
	sum(CASE WHEN NUMBER_OF_PERSONS_INJURED = 0 and NUMBER_OF_PERSONS_INJURED = 0 THEN 1 ELSE 0 END) AS minor,
	sum(CASE WHEN NUMBER_OF_PERSONS_INJURED > 0 or NUMBER_OF_PERSONS_INJURED > 0 THEN 1 ELSE 0 END) AS serious,
	count(*) AS incidents
	from collisions 
		where extract(month from datetime) = '12' and extract(year from datetime) = '2014' and extract(day from datetime)::integer >= 22
		group by extract(year from datetime), extract(month from datetime), extract(day from datetime)
		order by day;
endef
export DECANALYSISQUERY

define YOYCHANGEQUERY
	select extract(month from datetime) AS month, extract(day from datetime) AS day,
				 extract(doy from datetime) AS doy,
	       sum(CASE WHEN datetime > '2014-01-07' THEN 1 ELSE 0 END) AS this_year, sum(CASE WHEN datetime < '2014-01-06' THEN 1 ELSE 0 END) AS last_year,
	       ((sum(CASE WHEN datetime > '2014-01-07' THEN 1 ELSE 0 END) -  sum(CASE WHEN datetime < '2014-01-06' THEN 1 ELSE 0 END) )::float /  sum(CASE WHEN datetime < '2014-01-06' THEN 1 ELSE 0 END) )::float AS yoy_change
 		from collisions
		where datetime > '2013-01-06'
  	group by extract(month from datetime), extract(day from datetime), extract(doy from datetime)
  	order by month, day;
endef
export YOYCHANGEQUERY


define YOYCHANGEQUERYFORCHART
\COPY (
	WITH latest_date AS (SELECT date_trunc('day', max(datetime) + interval '1 day') - interval '364 days' as max from collisions)

	SELECT extract(week from (datetime - interval '12 days'))::integer AS woy,


	      /* date_trunc('day', max(datetime) + interval '1 day') - (date_trunc('day', min(datetime)) + interval '364 days') as asdf, */
				sum(CASE WHEN datetime > (SELECT max FROM latest_date) THEN 1 ELSE 0 END) AS this_year,
				sum(CASE WHEN datetime < (SELECT max FROM latest_date) THEN 1 ELSE 0 END) AS last_year,
	       ((sum(CASE WHEN datetime > (SELECT max FROM latest_date) THEN 1 ELSE 0 END) -  sum(CASE WHEN datetime < (SELECT max FROM latest_date) THEN 1 ELSE 0 END) )::float /  sum(CASE WHEN datetime < (SELECT max FROM latest_date) THEN 1 ELSE 0 END)::float) AS yoy_change,
	       
	      sum(CASE WHEN datetime > (SELECT max FROM latest_date) THEN NUMBER_OF_PERSONS_INJURED ELSE 0 END) AS this_year_inj,
				sum(CASE WHEN datetime < (SELECT max FROM latest_date) THEN NUMBER_OF_PERSONS_INJURED ELSE 0 END) AS last_year_inj,
	       ((sum(CASE WHEN datetime > (SELECT max FROM latest_date) THEN NUMBER_OF_PERSONS_INJURED ELSE 0 END) -  sum(CASE WHEN datetime < (SELECT max FROM latest_date) THEN NUMBER_OF_PERSONS_INJURED ELSE 0 END) )::float /  sum(CASE WHEN datetime < (SELECT max FROM latest_date) THEN NUMBER_OF_PERSONS_INJURED ELSE 0 END)::float) AS yoy_change_injured,

				sum(CASE WHEN datetime > (SELECT max FROM latest_date) THEN NUMBER_OF_PERSONS_KILLED ELSE 0 END) AS this_year_kill,
				sum(CASE WHEN datetime < (SELECT max FROM latest_date) THEN NUMBER_OF_PERSONS_KILLED ELSE 0 END) AS last_year_kill,
	       ((sum(CASE WHEN datetime > (SELECT max FROM latest_date) THEN NUMBER_OF_PERSONS_KILLED ELSE 0 END) -  sum(CASE WHEN datetime < (SELECT max FROM latest_date) THEN NUMBER_OF_PERSONS_KILLED ELSE 0 END) )::float /  sum(CASE WHEN datetime < (SELECT max FROM latest_date) THEN NUMBER_OF_PERSONS_KILLED ELSE 0 END)::float) AS yoy_change_kill,

	       date_trunc('day', max(datetime) + interval '1 day') AS this_year_start,
	       (date_trunc('day', min(datetime)) + interval '364 days') AS this_year_end,
	       (date_trunc('day', max(datetime) + interval '1 day') - interval '364 days') AS last_year_start,
	       date_trunc('day', min(datetime)) AS last_year_end




 		FROM collisions
		WHERE datetime > (SELECT max - INTERVAL '364 days' FROM latest_date)
  	GROUP BY extract(week FROM (datetime - interval '12 days'))::integer
  	ORDER BY woy
  	) to 'collision_change_yoy.csv' with csv header;
endef
export YOYCHANGEQUERYFORCHART

define PERPRECINCTQUERY
	CREATE TABLE per_precinct_collisions AS
	SELECT nyppbuffered.Precinct, 
		sum(NUMBER_OF_PERSONS_INJURED) AS injured, sum(NUMBER_OF_PERSONS_KILLED) AS killed, 
		sum(CASE WHEN NUMBER_OF_PERSONS_INJURED = 0 and NUMBER_OF_PERSONS_INJURED = 0 THEN 1 ELSE 0 END) AS minor,
		sum(CASE WHEN NUMBER_OF_PERSONS_INJURED > 0 or NUMBER_OF_PERSONS_INJURED > 0 THEN 1 ELSE 0 END) AS serious,
		count(*) AS incidents
	FROM collisions
	JOIN nyppbuffered ON ST_Contains(nyppbuffered.geom, collisions.geom)
	WHERE collisions.datetime > '2014-12-22' AND collisions.datetime < '2015-01-09'
	GROUP BY nyppbuffered.Precinct
endef
export PERPRECINCTQUERY


define YOYPERPRECINCTQUERY
	CREATE TABLE per_precinct_collisions AS
	SELECT nyppbuffered.Precinct, 
		sum(CASE WHEN collisions.datetime > '2014-12-01' THEN NUMBER_OF_PERSONS_INJURED ELSE 0 END) AS injured, 
		sum(CASE WHEN collisions.datetime > '2014-12-01' THEN NUMBER_OF_PERSONS_KILLED ELSE 0 END) AS killed, 
		sum(CASE WHEN collisions.datetime > '2014-12-01' AND NUMBER_OF_PERSONS_INJURED = 0 and NUMBER_OF_PERSONS_INJURED = 0 THEN 1 ELSE 0 END) AS minor,
		sum(CASE WHEN collisions.datetime > '2014-12-01' AND NUMBER_OF_PERSONS_INJURED > 0 or NUMBER_OF_PERSONS_INJURED > 0 THEN 1 ELSE 0 END) AS serious,
		sum(CASE WHEN collisions.datetime < '2014-12-01' THEN NUMBER_OF_PERSONS_INJURED ELSE 0 END) AS injured13, 
		sum(CASE WHEN collisions.datetime < '2014-12-01' THEN NUMBER_OF_PERSONS_KILLED ELSE 0 END) AS killed13, 
		sum(CASE WHEN collisions.datetime < '2014-12-01' AND NUMBER_OF_PERSONS_INJURED = 0 and NUMBER_OF_PERSONS_INJURED = 0 THEN 1 ELSE 0 END) AS minor13,
		sum(CASE WHEN collisions.datetime < '2014-12-01' AND NUMBER_OF_PERSONS_INJURED > 0 or NUMBER_OF_PERSONS_INJURED > 0 THEN 1 ELSE 0 END) AS serious13,
		sum(CASE WHEN collisions.datetime > '2014-12-01' THEN 1 ELSE 0 END) AS incidents,
		sum(CASE WHEN collisions.datetime < '2014-12-01' THEN 1 ELSE 0 END) AS incidents13
	FROM collisions
	JOIN nyppbuffered ON ST_Contains(nyppbuffered.geom, collisions.geom)
	WHERE (collisions.datetime > '2014-12-22' AND collisions.datetime < '2015-01-09') OR (collisions.datetime > '2013-12-22' AND collisions.datetime < '2014-01-09')
	GROUP BY nyppbuffered.Precinct
endef
export YOYPERPRECINCTQUERY



define BUFFERPRECINCTS
	CREATE TABLE nypp2263buffered AS
	SELECT *, ST_Buffer(ST_MakeValid(geom), 10) as the_geom FROM nypp2263;
	ALTER TABLE nypp2263buffered DROP COLUMN geom;
	ALTER TABLE nypp2263buffered RENAME COLUMN the_geom TO geom;
endef
export BUFFERPRECINCTS
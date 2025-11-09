#!/bin/bash
set -o errexit -o nounset

#curl --fail -A "waterwaymap.org" -LO https://www.naturalearthdata.com/http//www.naturalearthdata.com/download/10m/cultural/ne_10m_admin_0_countries_iso.zip
#aunpack ne_10m_admin_0_countries_iso.zip
#curl --fail -A "waterwaymap.org" -LO https://www.naturalearthdata.com/http//www.naturalearthdata.com/download/10m/cultural/ne_10m_admin_1_states_provinces.zip
#aunpack ne_10m_admin_1_states_provinces.zip
#
psql -c "drop table if exists admins cascade;"
ogr2ogr -f PostgreSQL PG: ne_10m_admin_0_countries_iso/ne_10m_admin_0_countries_iso.shp  -sql "select NAME,iso_a2_eh as iso, null as parent_iso, 0 as level from ne_10m_admin_0_countries_iso" -nlt MULTIPOLYGON -unsetFid -t_srs EPSG:4326 -lco GEOMETRY_NAME=geom -nln admins
ogr2ogr -f PostgreSQL PG: ne_10m_admin_1_states_provinces/ne_10m_admin_1_states_provinces.shp -sql "select NAME,iso_3166_2 as iso, iso_a2 as parent_iso, 1 as level from ne_10m_admin_1_states_provinces" -nlt MULTIPOLYGON -unsetFid -t_srs EPSG:4326 -nln admins -append

psql -Xe -c "drop table if exists planet_grouped_waterways cascade;"
psql -Xe -c "drop table if exists ww_rank_in_a cascade;"
ogr2ogr -f PostgreSQL PG: "$1" -nlt MULTILINESTRING -unsetFid -oo ARRAY_AS_STRING=YES -t_srs EPSG:4326 -lco GEOMETRY_NAME=geom -lco SCHEMA=public -nln planet_grouped_waterways
psql -Xe -c 'create index name on planet_grouped_waterways (tag_group_value);'
psql -Xe -c 'create index length on planet_grouped_waterways (length_m);'

psql -Xe -c "drop table if exists planet_longest_source_mouth_parts cascade;"
ogr2ogr -f PostgreSQL PG: "$2" -nlt MULTILINESTRING -unsetFid -oo ARRAY_AS_STRING=YES -t_srs EPSG:4326 -lco GEOMETRY_NAME=geom -lco SCHEMA=public -nln planet_longest_source_mouth_parts


psql -Xe -c "create table planet_longest_source_mouth as select
	river_system_mouth_source_nids_s as mouth_source_nids_s,
	any_value(river_system_internal_groupids) as internal_groupids,
	 any_value(river_system_length_m) as length_m,
	 any_value(river_system_source_nid) as source_nid,
	 any_value(river_system_mouth_nid) as mouth_nid,
	 any_value(river_system_mouth_source_nids) as mouth_source_nids,
	 any_value(river_system_names) as names,
	 any_value(river_system_names_s) as names_s
	 from planet_longest_source_mouth_parts
	group by river_system_mouth_source_nids_s ;"
psql -Xe -c "alter table planet_longest_source_mouth add primary key (mouth_source_nids_s)"

for C in river_system_internal_groupids river_system_length_m river_system_source_nid river_system_mouth_nid river_system_mouth_source_nids river_system_names river_system_names_s ; do
	psql -c "alter table planet_longest_source_mouth_parts drop column ${C};"
done
psql -Xe -c "alter table planet_longest_source_mouth_parts add foreign key (river_system_mouth_source_nids_s) references planet_longest_source_mouth (mouth_source_nids_s);"

psql -X -f ~/osm/waterwaymap.org/riversite_input_data_setup.sql

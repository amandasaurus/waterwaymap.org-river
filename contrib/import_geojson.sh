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

psql -X -f ~/osm/waterwaymap.org/riversite_input_data_setup.sql

after schema changes on init.cql: 
docker exec tcl_cassandra cqlsh -f /init.cql

ipaddress:
docker inspect tcl_cassandra | Select-String IPAddress

check tables with : 
cqlsh 172.21.0.12 -e "USE tcl; SELECT * FROM delay_stats;"


cqlsh 172.21.0.6 -e "USE tcl; SELECT window_start, ligne, avg_delay_min FROM delay_stats WHERE ligne IN ('C7', '93') AND window_start>'2026-03-23 09:00+0000' ALLOW FILTERING;"
geojson url for grafana:
https://data.grandlyon.com/geoserver/ogc/features/v1/collections/sytral:tcl_sytral.tclarret/items?crs=EPSG:4326
import overpy
import fiona
from fiona.crs import from_epsg
import os

api = overpy.Overpass()

schema = {'geometry': 'LineString', 'properties': {'Name': 'str:80'}}

path = ".env/env.list"

os.chdir(path)

### Highway ####



mHigh = api.query("""
(
node[highway=motorway](38.82011,-75.69062, 41.46490,-73.79485);
way[highway=motorway](38.82011,-75.69062, 41.46490,-73.79485);
rel[highway=motorway](38.82011,-75.69062, 41.46490,-73.79485);
);
(._;>;);
out center;
""")

mHighCrd  = []
mHighCrd += [(float(node.lat), float(node.lon))
           for node in mHigh.nodes]
mHighCrd += [(float(way.center_lat), float(way.center_lon))
           for way in mHigh.ways]
mHighCrd += [(float(rel.center_lat), float(rel.center_lon))
           for rel in mHigh.relations]

mHighCrdWay =[]
mHighCrdWay += [(float(way.center_lat), float(way.center_lon), int(way.id))
           for way in mHigh.ways]



shapeout = "mHigh.shp"
with fiona.open(shapeout, 'w', crs=from_epsg(4326), driver='ESRI Shapefile', schema=schema) as output:
    for way in mHigh.ways:
        # the shapefile geometry use (lon,lat)
        line = {'type': 'LineString', 'coordinates': [(node.lon, node.lat) for node in way.nodes]}
        prop = {'Name': way.tags.get("name", "n/a")}
        output.write({'geometry': line, 'properties': prop})



###
###  MEASURE DISTANCE
###


from geopy.distance import geodesic
from shapely.geometry import LineString, Point, LinearRing
import geopandas as gpd
import osmnx as ox
from dask.distributed import Client
from timeit import default_timer as timer
import pandas as pd
from shapely.ops import nearest_points
import dask_geopandas



Data = pd.read_csv("path/badcoordTEST.csv")


mHighShp = gpd.read_file("OSM/mHigh.shp").geometry.unary_union


geoBuff = gpd.read_file("bufNJ.shp")
geoBuff1 = geoBuff['geometry'].iloc[0]

client=Client(processes=False)


gdf_nodes = gpd.GeoDataFrame(data={'x':Data['longitude'], 'y':Data['latitude'], 'propertymuid':Data['propertymuid']})
gdf_nodes.crs = 6347
gdf_nodes.name = 'nodes'
gdf_nodes['geometry'] = gdf_nodes.apply(lambda row: Point((row['x'], row['y'])), axis=1)
west, south, east, north = gdf_nodes.unary_union.bounds

boundGrid = ox.utils_geo._quadrat_cut_geometry(geoBuff1, .2)

sindex = gdf_nodes.sindex

####
####   GET NEAREST POINTS
####


outDF = []
start = timer()
i=0
for poly in boundGrid:
    i += 1
    poly = poly.buffer(1e-14).buffer(0)
    big_poly = poly.buffer(.15).buffer(0)
    mHighShp2 = mHighShp[mHighShp.intersects(big_poly)]

    possible_matches_index = list(sindex.intersection(poly.bounds))
    possible_matches = gdf_nodes.iloc[possible_matches_index]
    precise_matches = possible_matches[possible_matches.intersects(poly)]


    precise_matches['nearestMotorway'] = dask_geopandas.from_geopandas(precise_matches, npartitions=60). \
        map_partitions(
        lambda df: df.apply(
            lambda y: nearest_points(mHighShp, y.geometry)[0].coords, axis=1)). \
        compute()
        
        outDF.append(precise_matches)


end = timer()
timeGetPoints = end-start

outDF1=pd.concat(outDF)

####
####  Create Float Coords
####
outDF1['mHigh_lon'] = outDF1.apply(lambda x: x.nearestMotorway[0][0], axis=1)
outDF1['mHigh_lat'] = outDF1.apply(lambda x: x.nearestMotorway[0][1], axis=1)

#####
#####  MEASURE DISTANCES
#####

start = timer()
outDF1['dist_nearestMotorway'] = dask_geopandas.from_geopandas(outDF1, npartitions=60). \
        map_partitions(
        lambda df: df.apply(
            lambda y: geodesic((y['x'], y['y']), (y['mHigh_lon'], y['mHigh_lat'])).mi, axis=1)). \
        compute()

end = timer()
time_measureDist=(end-start)

outDF1.to_csv("OSMDist.csv")







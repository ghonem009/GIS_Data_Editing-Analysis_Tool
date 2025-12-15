import geopandas as gpd 
from shapely.geometry import shape
from shapely.validation import make_valid
import pandas as pd 
import os 
from app.config import DATA_DIR


class GISManager:
    def __init__(self, crs="EPSG:4326"):\
        self.gdf = gpd.GeoDataFrame(column=["properties", "geometry"], crs=crs)



    def add_feature(self, geom_dict, properties: dict):
        geom = shape(geom_dict)
        if not geom.is_valid:
            geom = make_valid(geom)

        new_raw = {"properties": properties, "geometry": geom}
        self.gdf.loc[len(self.gdf)] = new_raw



    def delete_feature_by_index(self, index):
        if index in self.gdf.index:
            self.gdf = self.gdf[self.gdf.index != index] 
            print(f"this {index} has been successfully deleted")
            return True
        else:
            print(f" This {index} has not been found ")
            return False



    def reproject(self, target_crs ="EPSG:4326"):
        if self.gdf.crs != target_crs:
            self.gdf = self.gdf.to_crs(target_crs)
        print(f"Reprojected to {target_crs} is done ")



    def show_geoDF(self):
        print(self.gdf)



    def save(self, filename ="output.geojson"):
        path = os.path.join(DATA_DIR, filename)
        self.gdf.to_file(path, driver="Geojson")
        return path
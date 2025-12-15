import geopandas as gpd 
from shapely.geometry import shape
from shapely.validation import make_valid
import pandas as pd 
import os 
from app.config import DATA_DIR


class GISManager:
    def __init__(self, crs="EPSG:4326"):\
        self.gdf = gpd.GeoDataFrame(columns=["feature_id", "properties", "geometry"], crs=crs)


    # add feature
    def add_feature(self, geom_dict, properties: dict):
        geom = shape(geom_dict)

        if not geom.is_valid:
            geom = make_valid(geom)

        feature_id = len(self.gdf) + 1 
        new_row = {
            "feature_id": feature_id,
            "properties": properties,
            "geometry": geom
        }

        self.gdf.loc[len(self.gdf)] = new_row
        return feature_id




    # def delete_feature_by_index(self, index):
    #     if index in self.gdf.index:
    #         self.gdf = self.gdf[self.gdf.index != index] 
    #         print(f"this {index} has been successfully deleted")
    #         return True
    #     else:
    #         print(f" This {index} has not been found ")
    #         return False


    def delete_feature(self, index: int):
        if index in self.gdf.index:
            self.gdf = self.gdf.drop(index).reset_index(drop=True)
            return True
        return False

    # reproject to target_crs = EPSG:4326
    def reproject(self, target_crs ="EPSG:4326"):
        if self.gdf.crs != target_crs:
            self.gdf = self.gdf.to_crs(target_crs)
        print(f"Reprojected to {target_crs} is done ")


    # show_geoDF
    def show_geoDF(self):
        print(self.gdf)


    # save file 
    def save(self, filename ="output.geojson"):
        path = os.path.join(DATA_DIR, filename)
        self.gdf.to_file(path, driver="Geojson")
        return path

    # load_dataset
    def load_dataset(self, file_path: str):
        self.gdf = gpd.read_file(file_path)

        if self.gdf.crs is None:
            self.gdf.set_crs("EPSG:4326", inplace=True)

        return self.gdf


    # buffer
    def buffer(self, distance: float):
        self.gdf["geometry"] =  self.gdf.geometry.buffer(distance)
        


    # intersect
    def intersect(self, geom_dict: dict):
        geom = shape(geom_dict)
        geom = make_valid(geom) if not geom.is_valid else geom
        result = self.gdf[self.gdf.geometry.intersects(geom)]
        return result
    


    # clip
    def clip(self, geom_dict: dict):
        mask = shape(geom_dict)
        mask = make_valid(mask) if not mask.is_valid else mask
        clipped = gpd.clip(self.gdf, mask)
        return clipped
    


    # simplification =>> tolerance, simplify_coverage, simplify_boundary ==> TRUE
    # inner edges only =>>> simplify_boundary => fales 

    def simplification(self,tolerance: float, simplify_coverage: bool = True, simplify_boundary: bool = True ):
        if simplify_coverage:
            self.gdf["geometry"] = self.gdf.geometry.simplify_coverage(tolerance=tolerance, simplify_boundary=simplify_boundary)
        else:
            self.gdf["geometry"] = self.gdf.geometry.simplify(tolerance)
        return self.gdf
    


    # dissolve by attribute 
    def dissolve(self, by: str):
        self.gdf = self.gdf.dissolve(by=by, as_index=False)
        return self.gdf 
    



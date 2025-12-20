import geopandas as gpd 
from shapely.geometry import shape
from shapely.validation import make_valid
import pandas as pd 
import os 
from app.config import DATA_DIR
from app.core.geometry_utils import parse_geometry, validate_geometry_type



class GISManager:
    def __init__(self, crs="EPSG:4326"):
        self.gdf = gpd.GeoDataFrame(columns=["feature_id", "properties", "geometry"], crs=crs)


    # add feature
    def add_feature(self, geom_dict, properties: dict):
        geom = parse_geometry(geom_dict, fmt="geojson", fix_topology=True)
        validate_geometry_type(geom, allowed_types=["Point", "LineString", "Polygon"])
        feature_id = int(self.gdf["feature_id"].max() + 1) if not self.gdf.empty else 1
        new_row = {
            "feature_id": feature_id,
            "properties": properties,
            "geometry": geom
        }

        self.gdf.loc[len(self.gdf)] = new_row
        return feature_id


    def delete_feature(self, feature_id: int):
        before = len(self.gdf)
        self.gdf = self.gdf[self.gdf["feature_id"] != feature_id].reset_index(drop=True)
        return len(self.gdf) < before

    
    # reproject to target_crs = EPSG:4326
    def reproject(self, target_crs ="EPSG:4326"):
        if str(self.gdf.crs) != target_crs:
            self.gdf = self.gdf.to_crs(target_crs)
        print(f"Reprojected to {target_crs} is done ")


    # show_geoDF
    def show_geoDF(self):
        print(self.gdf)


    # save file 
    def save(self, filename ="output.geojson"):
        path = os.path.join(DATA_DIR, filename)
        self.gdf.to_file(path, driver="GeoJSON")
        return path


    # load_dataset
    def load_dataset(self, file_path: str, source_crs: str = None):
        self.gdf = gpd.read_file(file_path)
        if self.gdf.crs is None:
            if source_crs is None:
                raise ValueError("Please provide source_crs ")
            self.gdf.set_crs(source_crs, inplace=True)

        if str(self.gdf.crs) != "EPSG:4326":
            self.gdf = self.gdf.to_crs("EPSG:4326")
        return self.gdf


    # buffer
    def buffer(self, distance: float, feature_id: int = None):
        original_crs = self.gdf.crs
        projected = self.gdf.to_crs(epsg=32636)

        if feature_id is not None:
            mask = self.gdf["feature_id"] == feature_id
            self.gdf.loc[mask, "geometry"] = self.gdf.loc[mask, "geometry"].buffer(distance)
        else:
            self.gdf["geometry"] = self.gdf.geometry.buffer(distance)

        self.gdf = projected.to_crs(original_crs)
        return self.gdf
        


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
    
    # union 
    def union(self, feature_ids: list = None):
        if feature_ids:
            features = self.gdf[self.gdf['feature_id'].isin(feature_ids)]
        else:
            features = self.gdf
        
        if len(features) == 0:
            return None
        
        union_geom = features.geometry.unary_union
        return union_geom


    # spatial analysis 

    # 1- nearest_neighbor  
    def nearest_neighbor(self, geom_dict: dict):
        if self.gdf.empty:
            return None
        geom = shape(geom_dict)
        geom = make_valid(geom) if not geom.is_valid else geom

        original_crs = self.gdf.crs

        gdf_proj = self.gdf.to_crs(epsg=32636)
        geom_proj = gpd.GeoSeries([geom], crs=original_crs).to_crs(epsg=32636).iloc[0]

        distances = gdf_proj.geometry.distance(geom_proj)
        idx = distances.idxmin()

        row = self.gdf.loc[idx]

        return {
        "feature_id": int(row["feature_id"]),
        "properties": row["properties"],
        "geometry": row["geometry"].__geo_interface__,
        "distance_meters": float(distances.loc[idx])
    }


    
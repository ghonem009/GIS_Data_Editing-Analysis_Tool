import geopandas as gpd 
from shapely.geometry import shape
from shapely.validation import make_valid
import json 
from sqlalchemy.dialects.postgresql import JSONB
from geoalchemy2 import Geometry
from sqlalchemy import text
from app.config import engine
from app.core.geometry_utils import parse_geometry, validate_geometry_type


class GISManager:
    def __init__(self, crs="EPSG:4326"):

        self.engine = engine
        self.table_name = "features"
        self.crs = crs
        self.load_from_db()

    def load_from_db(self):

        try:
            self.gdf = gpd.read_postgis(
                f"SELECT * FROM {self.table_name}",
                self.engine,
                geom_col="geometry"
            )

            if self.gdf.crs is None:
                self.gdf.set_crs(epsg=4326, inplace=True)
            
            if "properties" in self.gdf.columns:
                # -> [string or list >> json object]
                def fix_properties(properties):
                    if isinstance(properties, dict):
                        return properties
                    if isinstance(properties, str):
                        try:
                            return json.loads(properties)
                        except Exception:
                            return {}
                    return {}
                
                self.gdf["properties"] = self.gdf["properties"].apply(fix_properties)

        except Exception:
            self.gdf = gpd.GeoDataFrame(
                columns=["feature_id", "properties", "geometry"],
                crs=self.crs
            )

    
    def save_to_db(self, update_only=False):
        if not update_only:
            self.gdf["properties"] = self.gdf["properties"].apply(
                lambda v: json.dumps(v, ensure_ascii=False) if isinstance(v, dict) else v
            )

            self.gdf.to_postgis(
                name=self.table_name,
                con=self.engine,
                if_exists="replace",
                index=False,
                dtype={
                    "geometry": Geometry("GEOMETRY", srid=4326),
                    "properties": JSONB()
                }
            )

        else:
            sql = text("""
                UPDATE features
                SET geometry = ST_GeomFromText(:geometry, 4326),
                    properties = :properties
                WHERE feature_id = :feature_id
            """)

            data = [
                {
                    "geometry": r.geometry.wkt,
                    "properties": json.dumps(r.properties, ensure_ascii=False)
                    if isinstance(r.properties, dict)
                    else r.properties,
                    "feature_id": int(r.feature_id)
                }
                for r in self.gdf.itertuples(index=False)
            ]

            with self.engine.begin() as conn:
                conn.execute(sql, data)

    # add feature
    def add_feature(self, geom_dict, properties: dict, fix_topology=False):
        geom = parse_geometry(geom_dict, fmt="geojson", fix_topology=fix_topology)
        validate_geometry_type(geom, allowed_types=["Point", "LineString", "Polygon"])
        feature_id = int(self.gdf["feature_id"].max() + 1) if not self.gdf.empty else 1
        new_row = {
            "feature_id": feature_id,
            "properties": properties,
            "geometry": geom
        }

        self.gdf.loc[len(self.gdf)] = new_row
        self.gdf.set_geometry("geometry", inplace=True)
        self.save_to_db()
        return feature_id


    def delete_feature(self, feature_id: int):
        before = len(self.gdf)
        self.gdf = self.gdf[self.gdf["feature_id"] != feature_id].reset_index(drop=True)
        return len(self.gdf) < before
    

    def update_feature(self, feature_id: int, new_geom=None, new_properties=None, fix_topology=False):
        mask = self.gdf["feature_id"] == feature_id
        if mask.sum() == 0:
            raise ValueError("feature not found")
        if new_geom is not None:
            geom = parse_geometry(new_geom, fix_topology=fix_topology)
            validate_geometry_type(geom, allowed_types=["Point", "LineString", "Polygon"])
            self.gdf.loc[mask, "geometry"] = geom
        if new_properties is not None:
            self.gdf.loc[mask, "properties"] = new_properties

        return feature_id


    
    # reproject to target_crs = EPSG:4326
    def reproject(self, target_crs ="EPSG:4326"):
        if str(self.gdf.crs) != target_crs:
            self.gdf = self.gdf.to_crs(target_crs)
        print(f"Reprojected to {target_crs} is done ")


    def buffer(self, distance: float, feature_id: int = None):
        if self.gdf.crs is None:
            self.gdf.set_crs(epsg=4326, inplace=True)

        self.load_from_db()

        original_crs = self.gdf.crs
        projected = self.gdf.to_crs(epsg=32636)

        if feature_id is not None:
            mask = projected["feature_id"] == feature_id
            if mask.sum() == 0:
                raise ValueError(f"Feature ID {feature_id} not found")
            projected.loc[mask, "geometry"] = projected.loc[mask, "geometry"].buffer(distance)
        else:
            projected["geometry"] = projected.geometry.buffer(distance)

        self.gdf = projected.to_crs(original_crs)

        self.save_to_db(update_only=True)

        return self.gdf

    
    # intersect
    def intersect(self, geom_dict: dict):
        geom = shape(geom_dict)
        geom = make_valid(geom) if not geom.is_valid else geom
        result = self.gdf[self.gdf.geometry.intersects(geom)]
        self.save_to_db(update_only=True)
        result.save_to_db(update_only=True)
        return result
    


    # clip
    def clip(self, geom_dict: dict):
        mask = shape(geom_dict)
        mask = make_valid(mask) if not mask.is_valid else mask
        clipped = gpd.clip(self.gdf, mask)
        self.save_to_db(update_only=True)
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


    # 2 - spatial_join
    def spatial_join(self, other_gdf, how="inner", predicate="intersects"):
        if self.gdf.empty or other_gdf.empty:
            return gpd.GeoDataFrame()  
        joined = gpd.sjoin(self.gdf, other_gdf, how=how, predicate=predicate)
        return joined
    


    # 3- summary_statistics
    def summary_statistics(self, feature_id: int = None):
        df = self.gdf if feature_id is None else self.gdf[self.gdf["feature_id"] == feature_id]
        stats = []
        for _, row in df.iterrows():
            geom = row.geometry
            stats.append({
                "feature_id": row.feature_id,
                "area": geom.area,
                "length": geom.length,
                "centroid": geom.centroid.__geo_interface__,
                "bounding_box": geom.bounds
            })
        return stats

                
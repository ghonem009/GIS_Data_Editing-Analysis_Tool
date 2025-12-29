import geopandas as gpd
from shapely.geometry import shape
from shapely.validation import make_valid
import json
import asyncio
from concurrent.futures import ThreadPoolExecutor
from sqlalchemy.dialects.postgresql import JSONB
from geoalchemy2 import Geometry
from sqlalchemy import text, Table, Column, Integer, String, DateTime, MetaData, ARRAY
from datetime import datetime
from app.config import sync_engine, async_engine
from app.core.geometry_utils import parse_geometry, validate_geometry_type


class GISManager:
    def __init__(self, crs="EPSG:4326"):

        self.sync_engine = sync_engine
        self.async_engine = async_engine
        self.features_table = "features"    
        self.results_table = "analysis_results"
        self.crs = crs
        self.executor = ThreadPoolExecutor(max_workers=4)
        self.gdf = None

    async def tables_exist(self):

        metadata = MetaData()

        Table(
            self.results_table,
            metadata,
            Column('result_id', Integer, primary_key=True, autoincrement=True),
            Column('source_feature_ids', ARRAY(Integer)),
            Column('geometry', Geometry('GEOMETRY', srid=4326)),
            Column('properties', JSONB)
        )

        async with self.async_engine.begin() as conn:
            await conn.run_sync(metadata.create_all)

    async def load_from_db(self, table_name=None):
        table = table_name or self.features_table

        def _load():
            try:
                gdf = gpd.read_postgis(
                    f"SELECT * FROM {table}",
                    self.sync_engine,
                    geom_col="geometry"
                )

                if gdf.crs is None:
                    gdf.set_crs(epsg=4326, inplace=True)

                if "properties" in gdf.columns:
                    # -> [string or list >> json object]  >> check if properties is dict or  str 
                    def fix_properties(properties):
                        if isinstance(properties, dict):
                            return properties
                        if isinstance(properties, str):
                            try:
                                return json.loads(properties)
                            except Exception:
                                return {}
                        return {}

                    gdf["properties"] = gdf["properties"].apply(fix_properties)

                return gdf

            except Exception:
                return gpd.GeoDataFrame(
                    columns=["feature_id", "properties", "geometry"],
                    crs=self.crs
                )

        loop = asyncio.get_running_loop()
        self.gdf = await loop.run_in_executor(self.executor, _load)
        return self.gdf

    async def save_to_db(self, update_only=False):
        if not update_only:
            def _save():
                temp_gdf = self.gdf.copy()
                temp_gdf["properties"] = temp_gdf["properties"].apply(
                    lambda v: json.dumps(v, ensure_ascii=False)
                    if isinstance(v, dict) else v
                )

                temp_gdf.to_postgis(
                    name=self.features_table,
                    con=self.sync_engine,
                    if_exists="replace",
                    index=False,
                    dtype={
                        "geometry": Geometry("GEOMETRY", srid=4326),
                        "properties": JSONB()
                    }
                )

            loop = asyncio.get_running_loop()
            await loop.run_in_executor(self.executor, _save)

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

            async with self.async_engine.begin() as conn:
                await conn.execute(sql, data)

    # add feature
    async def add_feature(self, geom_dict, properties: dict, fix_topology=False):
        geom = parse_geometry(geom_dict, fmt="geojson", fix_topology=fix_topology)
        validate_geometry_type(geom, allowed_types=["Point", "LineString", "Polygon"])

        await self.load_from_db()

        feature_id = int(self.gdf["feature_id"].max() + 1) if not self.gdf.empty else 1
        new_row = {
            "feature_id": feature_id,
            "properties": properties,
            "geometry": geom
        }

        self.gdf.loc[len(self.gdf)] = new_row
        self.gdf.set_geometry("geometry", inplace=True)

        await self.save_to_db()
        return feature_id

    # buffer
    async def buffer(self, distance: float, feature_id: int = None):

        await self.load_from_db()

        if feature_id:
            target = self.gdf[self.gdf["feature_id"] == feature_id]
            if target.empty:
                raise ValueError(f"Feature ID {feature_id} not found")
        else:
            target = self.gdf

        def _buffer():
            result_gdf = target.copy()
            result_gdf["geometry"] = result_gdf.geometry.buffer(distance / 100000.0)
            return result_gdf

        loop = asyncio.get_running_loop()
        result_gdf = await loop.run_in_executor(self.executor, _buffer)

        async with self.async_engine.begin() as conn:
            for _, row in result_gdf.iterrows():
                await conn.execute(
                    text("""
                        INSERT INTO analysis_results
                        (source_feature_ids, geometry, properties)
                        VALUES (:source_feature_ids, ST_GeomFromText(:geometry, 4326), :properties)
                    """),
                    {
                        "source_feature_ids": [int(row["feature_id"])],
                        "geometry": row.geometry.wkt,
                        "properties": json.dumps(row.properties, ensure_ascii=False)
                    }
                )

            result_id = await conn.scalar(
                text("SELECT MAX(result_id) FROM analysis_results")
            )

        return result_id, result_gdf
    
    def get_analysis_results(self, result_id: int = None):
        try:
            query = f"SELECT * FROM {self.results_table}"
            if result_id:
                query += f" WHERE result_id = {result_id}"
            
            results = gpd.read_postgis(query, self.engine, geom_col="geometry")
            return results
        except:
            return gpd.GeoDataFrame()


    # intersect
    def intersect(self, geom_dict: dict):
        '''read only operation / query operation
         -> find feature that with given geometry'''
        geom = shape(geom_dict)
        geom = make_valid(geom) if not geom.is_valid else geom
        result = self.gdf[self.gdf.geometry.intersects(geom)]
        # self.save_to_db(update_only=True) - > no save needed 
        return result
        


    # clip
    def clip(self, geom_dict: dict):
        ''' clip feature to mask and save in db'''
        mask = shape(geom_dict)
        mask = make_valid(mask) if not mask.is_valid else mask
        clipped = gpd.clip(self.gdf, mask)
        self.gdf = clipped
        self.save_to_db()
        return clipped
    


    # simplification =>> tolerance, simplify_coverage, simplify_boundary ==> TRUE
    # inner edges only =>>> simplify_boundary => false 

    def simplification(self,tolerance: float, simplify_coverage: bool = True, simplify_boundary: bool = True ):
        "simplify geometry and save in db"
        if simplify_coverage:
            self.gdf["geometry"] = self.gdf.geometry.simplify_coverage(tolerance=tolerance, simplify_boundary=simplify_boundary)
        else:
            self.gdf["geometry"] = self.gdf.geometry.simplify(tolerance)
        self.save_to_db(update_only=True)
        return self.gdf
    
     
    def dissolve(self, by: str):
        '''dissolve by attribute and sve result'''
        self.gdf = self.gdf.dissolve(by=by, as_index=False)
        return self.gdf 
    
    
    def union(self, feature_ids: list = None):
        '''union feature -> read only'''
        if feature_ids:
            features = self.gdf[self.gdf['feature_id'].isin(feature_ids)]
        else:
            features = self.gdf
        
        if len(features) == 0:
            return None
        
        union_geom = features.geometry.unary_union
        return union_geom


    # spatial analysis 
    def nearest_neighbor(self, geom_dict: dict):
        '''find nearest feature -> read only'''
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
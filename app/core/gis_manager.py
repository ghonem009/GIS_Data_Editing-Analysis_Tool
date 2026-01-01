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
        """
        initialize the GISManager

        args:
            crs (str): default coordinate reference system (default: EPSG:4326)
        """
        self.sync_engine = sync_engine
        self.async_engine = async_engine
        self.features_table = "features"
        self.results_table = "analysis_results"
        self.crs = crs
        self.executor = ThreadPoolExecutor(max_workers=4)
        self.gdf = None

    # async def tables_exist(self):
    #     """
    #     ensure required database tables exist (features and analysis_results)
    #     Creates them if not already present
    #     """
    #     metadata = MetaData()
    #     Table(
    #         self.results_table,
    #         metadata,
    #         Column('result_id', Integer, primary_key=True, autoincrement=True),
    #         Column('source_feature_ids', ARRAY(Integer)),
    #         Column('geometry', Geometry('GEOMETRY', srid=4326)),
    #         Column('properties', JSONB)
    #     )

    #     async with self.async_engine.begin() as conn:
    #         await conn.run_sync(metadata.create_all)

    async def tables_exist(self):
        """
        Ensure required database tables exist (features and analysis_results)
        Creates them if not already present
        """
        metadata = MetaData()

        # features table
        Table(
            self.features_table,
            metadata,
            Column('feature_id', Integer, primary_key=True, autoincrement=True),
            Column('properties', JSONB),
            Column('geometry', Geometry('GEOMETRY', srid=4326))
        )

        # analysis_results table with all columns used in analysis functions
        Table(
            self.results_table,
            metadata,
            Column('result_id', Integer, primary_key=True, autoincrement=True),
            Column('operation_type', String(50)),
            Column('source_feature_ids', ARRAY(Integer)),
            Column('parameters', JSONB),
            Column('description', String(255)),
            Column('geometry', Geometry('GEOMETRY', srid=4326)),
            Column('properties', JSONB),
            Column('created_at', DateTime, default=datetime.utcnow)
        )

        async with self.async_engine.begin() as conn:
            await conn.run_sync(metadata.create_all)

    async def load_from_db(self, table_name=None):
        """
        load features or results from the database into a gdf

        args:
            table_name (str, optional): Name of the table to load (default: features).
        Returns:
            GeoDataFrame: Loaded spatial features with geometry and properties.
        """
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

                def fix_properties(properties):
                    if isinstance(properties, dict):
                        return properties
                    if isinstance(properties, str):
                        try:
                            return json.loads(properties)
                        except Exception:
                            return {}
                    return {}

                if "properties" in gdf.columns:
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
        """
        save GeoDataFrame to the database 

        args:
            update_only (bool): If True only updates existing records; otherwise replaces the table
        """
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


    async def add_feature(self, geom_dict, properties: dict, fix_topology=False):
        """
        Add a new feature to the dataset.

        args:
            geom_dict (dict): Geometry in GeoJSON format.
            properties (dict): Feature attributes.
            fix_topology (bool): Auto-fix invalid geometries.

        returns:
            int: Newly created feature ID.
        """
        geom = parse_geometry(geom_dict, fmt="geojson", fix_topology=fix_topology)
        validate_geometry_type(geom, allowed_types=["Point", "LineString", "Polygon"])

        await self.load_from_db()
        feature_id = int(self.gdf["feature_id"].max() + 1) if not self.gdf.empty else 1

        self.gdf.loc[len(self.gdf)] = {
            "feature_id": feature_id,
            "properties": properties,
            "geometry": geom
        }

        self.gdf.set_geometry("geometry", inplace=True)
        await self.save_to_db()
        return feature_id


    async def update_feature(self, feature_id: int, new_geom=None, new_properties=None, fix_topology=False):
        """
        update features [geometry / properties] and save to db
        """
        await self.load_from_db()
        if self.gdf.empty or feature_id not in self.gdf["feature_id"].values:
            raise ValueError("Feature not found")

        idx = self.gdf[self.gdf["feature_id"] == feature_id].index[0]

        if new_geom:
            geom = parse_geometry(new_geom, fmt="geojson", fix_topology=fix_topology)
            validate_geometry_type(geom, allowed_types=["Point", "LineString", "Polygon"])
            self.gdf.at[idx, "geometry"] = geom

        if new_properties:
            self.gdf.at[idx, "properties"] = new_properties

        await self.save_to_db(update_only=True)
        return feature_id


    async def delete_feature(self, feature_id: int):
        """
        delete feature and save to db
        """
        await self.load_from_db()
        before = len(self.gdf)
        self.gdf = self.gdf[self.gdf["feature_id"] != feature_id].reset_index(drop=True)
        deleted = len(self.gdf) < before
        if deleted:
            await self.save_to_db()  
        return deleted


    async def buffer(self, distance: float, feature_id: int = None):
        """
        create buffer for a feature or all features
        """
        await self.load_from_db()
        target = self.gdf[self.gdf["feature_id"] == feature_id] if feature_id else self.gdf

        if target.empty:
            raise ValueError(f"Feature ID {feature_id} not found")

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
    
    async def clip(self, geom_dict: dict, description: str = None):
        """Clip features and save result to analysis_results"""
        await self.load_from_db()

        def _clip():
            mask = shape(geom_dict)
            mask = make_valid(mask) if not mask.is_valid else mask
            clipped = gpd.clip(self.gdf, mask)
            source_ids = self.gdf["feature_id"].tolist()
            return clipped, source_ids

        loop = asyncio.get_running_loop()
        clipped, source_ids = await loop.run_in_executor(self.executor, _clip)

        async with self.async_engine.begin() as conn:
            for _, row in clipped.iterrows():
                await conn.execute(
                    text("""
                        INSERT INTO analysis_results 
                        (operation_type, source_feature_ids, parameters, description, geometry, properties)
                        VALUES (:op, :ids, :params, :desc, ST_GeomFromText(:geom, 4326), :props)
                    """),
                    {
                        "op": "clip",
                        "ids": source_ids,
                        "params": json.dumps({"clip_geometry": geom_dict}),
                        "desc": description,
                        "geom": row.geometry.wkt,
                        "props": json.dumps(row.properties, ensure_ascii=False)
                    }
                )
            result = await conn.execute(text("SELECT MAX(result_id) FROM analysis_results"))
            result_id = result.scalar()
        return result_id, clipped

    async def simplification(self, tolerance: float, feature_ids: list = None, 
                            simplify_coverage: bool = True, simplify_boundary: bool = True,
                            description: str = None):
        """Simplify geometries and save result"""
        await self.load_from_db()

        def _simplify():

            if feature_ids:
                work_gdf = self.gdf[self.gdf["feature_id"].isin(feature_ids)].copy()
                source_ids = feature_ids
            else:
                work_gdf = self.gdf.copy()
                source_ids = work_gdf["feature_id"].tolist()

            work_gdf["geometry"] = work_gdf.geometry.simplify(
                tolerance=tolerance,
                preserve_topology=simplify_boundary
            )
            return work_gdf, source_ids


        loop = asyncio.get_running_loop()
        simplified, source_ids = await loop.run_in_executor(self.executor, _simplify)

        async with self.async_engine.begin() as conn:
            for _, row in simplified.iterrows():
                await conn.execute(
                    text("""
                        INSERT INTO analysis_results 
                        (operation_type, source_feature_ids, parameters, description, geometry, properties)
                        VALUES (:op, :ids, :params, :desc, ST_GeomFromText(:geom, 4326), :props)
                    """),
                    {
                        "op": "simplify",
                        "ids": source_ids,
                        "params": json.dumps({
                            "tolerance": tolerance,
                            "simplify_coverage": simplify_coverage,
                            "simplify_boundary": simplify_boundary
                        }),
                        "desc": description,
                        "geom": row.geometry.wkt,
                        "props": json.dumps(row.properties, ensure_ascii=False)
                    }
                )
            result = await conn.execute(text("SELECT MAX(result_id) FROM analysis_results"))
            result_id = result.scalar()
        return result_id, simplified

    async def dissolve(self, by: str, feature_ids: list = None, description: str = None):
        """Dissolve features by attribute and save result"""
        await self.load_from_db()
        if by not in self.gdf.columns:
    # try to extract it manually from nested properties
            self.gdf[by] = self.gdf["properties"].apply(
                lambda p: p.get("properties", {}).get(by) if isinstance(p, dict) else None
    )

        def _dissolve():
            if feature_ids:
                work_gdf = self.gdf[self.gdf["feature_id"].isin(feature_ids)].copy()
                source_ids = feature_ids
            else:
                work_gdf = self.gdf.copy()
                source_ids = work_gdf["feature_id"].tolist()
            dissolved = work_gdf.dissolve(by=by, as_index=False)
            return dissolved, source_ids

        loop = asyncio.get_running_loop()
        dissolved, source_ids = await loop.run_in_executor(self.executor, _dissolve)

        async with self.async_engine.begin() as conn:
            for _, row in dissolved.iterrows():
                await conn.execute(
                    text("""
                        INSERT INTO analysis_results 
                        (operation_type, source_feature_ids, parameters, description, geometry, properties)
                        VALUES (:op, :ids, :params, :desc, ST_GeomFromText(:geom, 4326), :props)
                    """),
                    {
                        "op": "dissolve",
                        "ids": source_ids,
                        "params": json.dumps({"dissolve_by": by}),
                        "desc": description,
                        "geom": row.geometry.wkt,
                        "props": json.dumps(row.properties, ensure_ascii=False)
                    }
                )
            result = await conn.execute(text("SELECT MAX(result_id) FROM analysis_results"))
            result_id = result.scalar()
        return result_id, dissolved

    async def union(self, feature_ids: list = None):
        """Union multiple features"""
        await self.load_from_db()

        def _union():
            if feature_ids:
                features = self.gdf[self.gdf['feature_id'].isin(feature_ids)]
            else:
                features = self.gdf
            if len(features) == 0:
                return None
            return features.geometry.unary_union

        loop = asyncio.get_running_loop()
        union_geom = await loop.run_in_executor(self.executor, _union)
        return union_geom

    async def nearest_neighbor(self, geom_dict: dict):
        """Find nearest feature to given geometry"""
        await self.load_from_db()
        if self.gdf.empty:
            return None

        def _nearest():
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

        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(self.executor, _nearest)

    async def spatial_join(self, other_gdf, how="inner", predicate="intersects"):
        """Perform spatial join with another GeoDataFrame"""
        await self.load_from_db()
        if self.gdf.empty or other_gdf.empty:
            return gpd.GeoDataFrame()

        def _join():
            return gpd.sjoin(self.gdf, other_gdf, how=how, predicate=predicate)

        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(self.executor, _join)

    async def get_analysis_results(self, result_id: int = None, operation_type: str = None):
        """Retrieve analysis results"""
        query = f"SELECT * FROM {self.results_table} WHERE 1=1"
        if result_id:
            query += f" AND result_id = {result_id}"
        if operation_type:
            query += f" AND operation_type = '{operation_type}'"
        query += " ORDER BY created_at DESC"

        def _load_results():
            try:
                return gpd.read_postgis(query, self.sync_engine, geom_col="geometry")
            except:
                return gpd.GeoDataFrame()

        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(self.executor, _load_results)

    async def delete_analysis_result(self, result_id: int):
        """Delete an analysis result"""
        async with self.async_engine.begin() as conn:
            result = await conn.execute(
                text(f"DELETE FROM {self.results_table} WHERE result_id = :id"),
                {"id": result_id}
            )
            return result.rowcount > 0
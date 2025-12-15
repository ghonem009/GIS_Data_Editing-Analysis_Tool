# GIS Data Editing & Analysis Tool

## Overview

This project is a backend GIS tool built using Python. The main goal is to provide API endpoints that allow users to upload, edit, update, analyze, and manage GIS vector and raster data.

The tool is designed to work with an existing geospatial stack and focuses on clean data editing, validation, and basic spatial analysis through simple REST APIs.

## Core Responsibilities

### Data Input and Validation

The backend provides APIs to:

* Upload GIS files such as Shapefile, GeoJSON, KML, GPKG, and TIFF.
* Accept raw geometries from the frontend in formats like WKT, WKB, or GeoJSON.

Validation includes:

* Checking geometry types (Point, LineString, Polygon).
* Detecting coordinate reference systems and reprojecting when needed.
* Fixing invalid geometries and basic topology issues.

### Vector Data Editing Features

The tool supports common vector editing operations, including:

Add Feature

* Add new geometries with attributes to an existing dataset.

Delete Feature

* Delete features by ID or using spatial queries such as intersection or bounding box.

Update Feature

* Edit geometry coordinates.
* Update feature attributes.
* Merge or split geometries.
* Reproject datasets when required.

Geometry Operations

* Buffer creation.
* Clip operations.
* Union and intersection.
* Geometry simplification or smoothing.
* Dissolve features based on an attribute.

### Raster Handling

When raster data or DEM files are included, the backend can:

* Load raster files using rasterio.
* Extract pixel values from given coordinates.
* Clip rasters using polygon masks.
* Reproject raster datasets.
* Optionally generate hillshade or colored DEM outputs.

### Spatial Analysis Features

The backend provides spatial analysis tools such as:

* Nearest neighbor search.
* Spatial joins (point in polygon and polygon intersection).
* Buffer-based spatial queries.
* Feature statistics like area, length, centroid, and bounding box.
* Raster statistics including minimum, maximum, mean values, and histograms.

## API Requirements

The APIs are built using FastAPI and follow REST principles.

### Dataset Management

* POST /dataset/upload
* GET /dataset/list
* DELETE /dataset/{id}

### Feature Editing

* POST /feature/add
* PUT /feature/{id}/update
* DELETE /feature/{id}/delete

### Spatial Analysis

* POST /analysis/buffer
* POST /analysis/intersect
* POST /analysis/nearest
* POST /analysis/spatial-join

### Raster Operations

* POST /raster/value
* POST /raster/clip

Each endpoint:

* Validates input data.
* Returns clear JSON responses.
* Logs operations and errors.

## Performance

* Lightweight operations are handled in memory using GeoPandas and Shapely.
* Heavy processing is delegated to async workers or PostGIS when needed.
* Spatial indexes, caching, and chunked raster processing are used for optimization.

## Error Handling

* Invalid geometries return clear error messages.
* Unsupported CRS values are reprojected automatically or reported.
* File parsing issues return detailed error information.

## Deliverables

* Fully implemented Python backend module.
* Clean and documented API endpoints.
* This README explaining setup, supported formats, and example usage.

## How to Run

1. Install Python dependencies.
2. Start the FastAPI service.
3. Use the API endpoints to upload and manage GIS data.

## Supported Formats

* Vector: Shapefile, GeoJSON, KML, GPKG
* Raster: GeoTIFF

## Example Usage

* Upload a dataset using the upload endpoint.
* Edit or analyze features through the feature and analysis APIs.
* Retrieve results as JSON responses.

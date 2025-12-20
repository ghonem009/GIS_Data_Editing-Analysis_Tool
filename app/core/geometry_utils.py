from shapely.geometry import shape
from shapely import wkt, wkb
from shapely.validation import make_valid

def parse_geometry(
    geometry_input, fmt: str = "geojson", fix_topology: bool = False):

    if fmt == "geojson":
        geom = shape(geometry_input)

    elif fmt == "wkt":
        geom = wkt.loads(geometry_input)

    elif fmt == "wkb":
        geom = wkb.loads(bytes.fromhex(geometry_input))

    else:
        raise ValueError("unsupported geometry format")

    if geom.is_empty:
        raise ValueError("empty geometry is not allowed")

    if not geom.is_valid:
        if fix_topology:
            geom = make_valid(geom)
        else:
            raise ValueError("invalid geometry topology")

    return geom


def validate_geometry_type(geom, allowed_types: list):
    if geom.geom_type not in allowed_types:
        raise ValueError(
            f"Invalid geometry type: {geom.geom_type}. "
            f"Allowed: {allowed_types}"
        )

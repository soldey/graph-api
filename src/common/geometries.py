import json
from typing import Literal, Any, Optional

import shapely
import shapely.geometry as geom
from pydantic import BaseModel, Field


class Geometry(BaseModel):
    """
    Geometry representation for GeoJSON model.
    """

    type: Literal["Point", "Polygon", "MultiPolygon", "LineString", "MultiLineString"] = Field(default="Polygon")
    coordinates: list[Any] = Field(
        description="list[int] for Point,\n" "list[list[list[int]]] for Polygon",
        default=[
            [
                [30.22, 59.86],
                [30.22, 59.85],
                [30.25, 59.85],
                [30.25, 59.86],
                [30.22, 59.86],
            ]
        ],
    )
    _shapely_geom: geom.Point | geom.Polygon | geom.MultiPolygon | geom.LineString | geom.MultiLineString | None = None

    def as_shapely_geometry(
        self,
    ) -> geom.Point | geom.Polygon | geom.MultiPolygon | geom.LineString | geom.MultiLineString:
        """
        Return Shapely geometry object from the parsed geometry.
        """
        if self._shapely_geom is None:
            self._shapely_geom = shapely.from_geojson(json.dumps({"type": self.type, "coordinates": self.coordinates}))
        return self._shapely_geom

    @classmethod
    def from_shapely_geometry(
        cls, geometry: geom.Point | geom.Polygon | geom.MultiPolygon | geom.LineString | geom.MultiLineString | None
    ) -> Optional["Geometry"]:
        """
        Construct Geometry model from shapely geometry.
        """
        if geometry is None:
            return None
        return cls(**geom.mapping(geometry))

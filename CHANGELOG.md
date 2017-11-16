## 3.0.0

- Crop lines to area of interest in RasterLinesJoin

## 3.0.0-beta-3

- Add optional boolean parameter `pixelIsArea` which can be used
  to treat the pixels of a raster as an area instead of a point.
  This is useful when dealing with very large pixels of a low
  resolution raster and small input polygons.

## 3.0.0-beta-2

- Fetch tiles via Futures to parallelize IO

## 3.0.0-beta-1

- Add RasterLinesJoin geoprocessing operation

## 3.0.0-alpha-2

- Add RasterGroupedCount and RasterGroupedAverage geoprocessing operations

## 3.0.0-alpha

- Create an Akka HTTP server project in preparation for using the GeoTrellis
collections API and removing Spark Job Server.

## 2.1.0

- Standardize incoming geometries to `MapshedJob`, to account for any subtle
  geometric errors when supplying multiple, closely overlapping polygons. This
  does not change the output of any existing successful operations.

## 2.0.1

- Update GitHub OAuth token for JAR publishing.

## 2.0.0

- GeoTrellis S3 catalog has been changed to `datahub-catalogs-us-east-1`.
- Update Kryo registrator in default configuration.

## 1.2.0

- **BREAKING CHANGE** `RasterJoin` has been renamed to `RasterGroupedCount` to
  better match other similar new operations.
- Add `RasterGroupedSum` operation which takes a polygon, a set of rasters, and
  a target raster, and returns the sum of value of the target raster, grouped
  by the values of the list of rasters, clipped to the polygon.
- Add `RasterGroupedAverage` operation which takes a polygon, a set of rasters,
  and a target raster, and returns the average of the target raster, grouped by
  the values of the list of rasters, clipped to the polygon. If this endpoint
  is given an empty list of rasters, it will perform an ungrouped average over
  the entirety of the target raster clipped to the polygon.

## 1.1.0

- Add `MapshedJob` class to handle requests for MapShed. This class supports
  three kinds of operations:
  - `RasterLinesJoin`: This operation takes a polygon, a set of vectors, and
    a set of rasters, and returns a histogram containing tuples of raster
    values mapped to the count of vector cells intersecting them. Under the
    hood this builds an R-Tree to bucket vectors into tile extents before
    matching them with the individual tiles. We construct an RDD from this
    R-Tree and process the intersection parallely.
  - `RasterLinesJoinSequential`: This operation performs the same task as the
    previous one, but under the hood it does not convert the R-Tree to an RDD,
    performing the task sequentially instead. These two methods exist so that
    the client may choose which implementation to use, given the constraints
    of input and infrastructure.
  - `RasterJoin`: This operation takes a polygon and a set of rasters, and
    returns a histogram containing tuples of raster values mapped to the count
    of polygon cells intersecting them. This does not construct an R-Tree since
    the number of polygons and rasters is much smaller than the number of
    vectors in the input.


## 1.0.0

- Update GeoTrellis dependency to `0.10.0`

## 0.4.0

- Update GeoTrellis dependency to `0.10.0-177004b`.
- Update default Spark (`1.5.2`) and Spark Job Server (`0.6.1`) dependencies.

## 0.3.2

- Make soil type 'C', instead of 'B', the default value when data is missing.

## 0.3.1

- Functionally equivalent to `0.3.0`. Version bump was to deal with
  git flow release issues.

## 0.3.0

- The section about building Geotrellis locally has been removed from the README.
- The repository from which the GDAL dependency is pulled has been changed.
- The code has been updated to support the new soil and NLCD tiles.

## 0.2.0

- Only download tiles once for every (multi)polygon in the input list.

## 0.1.1

- Functionally equivalent to `0.1.0`. Version bump was to deal with Travis CI
  release issues.

## 0.1.0

- Initial release.

# SACZ area boundary data

This directory contains the area polygons used to calculate the spatial means of the SACZ predictors:

```text
sams_index_calc_areas.shp
sams_index_calc_areas.shx
sams_index_calc_areas.dbf
sams_index_calc_areas.cpg
```

The shapefile contains 69 valid polygons and an `area` attribute with the unique identifiers referenced by the internal predictor catalog. Its coordinates use longitudes from 0 to 360 degrees, and the DBF encoding is `ISO-8859-1`.

The source bundle has no `.prj` file, so it does not declare a CRS. The workflow preserves the source implementation's spatial assumptions when clipping atmospheric fields.

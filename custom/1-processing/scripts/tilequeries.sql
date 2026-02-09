INSTALL spatial;
LOAD spatial; -- noqa
INSTALL httpfs;
LOAD httpfs;

SET s3_region='us-west-2';

-- Set temp directory to use POSIX paths (critical for WSL compatibility)
-- Directory is created by downloadOverture.py before executing this SQL
SET temp_directory='{{duckdb_temp_dir}}';

-- Configure Overture Maps release version
SET VARIABLE overture_release = '2026-01-21.0';

----------------------------------------------------------------------
-- 1. LAND USE (NON-RESIDENTIAL)
----------------------------------------------------------------------

-- COPY (
--     SELECT
--         id,
--         subtype,
--         class,
--         surface,
--         names,
--         geometry  -- DuckDB v1.1.0+ treats this as GEOMETRY
--     FROM read_parquet(
--         's3://overturemaps-us-west-2/release/' ||
--         getvariable('overture_release') ||
--         '/theme=base/type=land_use/*',
--         filename = true,
--         hive_partitioning = 1
--     )
--     WHERE
--         subtype <> 'residential'
--         AND bbox.xmin < $extent_xmax AND bbox.xmax > $extent_xmin
--         AND bbox.ymin < $extent_ymax AND bbox.ymax > $extent_ymin
-- ) TO '{{overture_data_dir}}/land_use.parquet'
-- WITH (FORMAT PARQUET, COMPRESSION 'ZSTD');

-- break

----------------------------------------------------------------------
-- 2. LAND USE (RESIDENTIAL ONLY)
----------------------------------------------------------------------

COPY (
    SELECT
        id,
        subtype,
        class,
        surface,
        names,
        geometry
    FROM read_parquet(
        's3://overturemaps-us-west-2/release/' ||
        getvariable('overture_release') ||
        '/theme=base/type=land_use/*',
        filename = true,
        hive_partitioning = 1
    )
    WHERE
        subtype = 'residential'
        AND bbox.xmin < $extent_xmax AND bbox.xmax > $extent_xmin
        AND bbox.ymin < $extent_ymax AND bbox.ymax > $extent_ymin
) TO '{{overture_data_dir}}/land_residential.parquet'
WITH (FORMAT PARQUET, COMPRESSION 'ZSTD');

-- break

----------------------------------------------------------------------
-- 3. WATER
--        subtype, class, names, is_salt, is_intermittent, etc.)
----------------------------------------------------------------------

COPY (
    SELECT
        id,
        subtype,
        class,
        names,
        is_salt,
        is_intermittent,
        geometry
    FROM read_parquet(
        's3://overturemaps-us-west-2/release/' ||
        getvariable('overture_release') ||
        '/theme=base/type=water/*',
        filename = true,
        hive_partitioning = 1
    )
    WHERE
        subtype IN ('ocean', 'lake', 'pond', 'reservoir', 'river', 'stream', 'water', 'canal')
        AND bbox.xmin < $extent_xmax AND bbox.xmax > $extent_xmin
        AND bbox.ymin < $extent_ymax AND bbox.ymax > $extent_ymin
) TO '{{overture_data_dir}}/water.parquet'
WITH (FORMAT PARQUET, COMPRESSION 'ZSTD');

-- break

----------------------------------------------------------------------
-- 4. ROADS (TRANSPORTATION / SEGMENTS, subtype = 'road')
----------------------------------------------------------------------

COPY (
    SELECT
        id,
        subtype,
        class,
        subclass,
        names,
        geometry
    FROM read_parquet(
        's3://overturemaps-us-west-2/release/' ||
        getvariable('overture_release') ||
        '/theme=transportation/type=segment/*',
        filename = true,
        hive_partitioning = 1
    )
    WHERE
        subtype = 'road'
        AND bbox.xmin < $extent_xmax AND bbox.xmax > $extent_xmin
        AND bbox.ymin < $extent_ymax AND bbox.ymax > $extent_ymin
) TO '{{overture_data_dir}}/roads.parquet'
WITH (FORMAT PARQUET, COMPRESSION 'ZSTD');

-- break

----------------------------------------------------------------------
-- 5. BUILDINGS
----------------------------------------------------------------------

-- COPY (
--     SELECT
--         id,
--         subtype,
--         class,
--         names,
--         level,
--         height,
--         num_floors,
--         num_floors_underground,
--         geometry
--     FROM read_parquet(
--         's3://overturemaps-us-west-2/release/' ||
--         getvariable('overture_release') ||
--         '/theme=buildings/type=building/*',
--         filename = true,
--         hive_partitioning = 1
--     )
--     WHERE
--         bbox.xmin < $extent_xmax AND bbox.xmax > $extent_xmin
--         AND bbox.ymin < $extent_ymax AND bbox.ymax > $extent_ymin
-- ) TO '{{overture_data_dir}}/buildings.parquet'
-- WITH (FORMAT PARQUET, COMPRESSION 'ZSTD');

-- break

----------------------------------------------------------------------
-- 6. LAND COVER
--   cartography.min_zoom, cartography.max_zoom, cartography.sort_key
----------------------------------------------------------------------

COPY (
    SELECT
        id,
        subtype,
        cartography.min_zoom  AS min_zoom,
        cartography.max_zoom  AS max_zoom,
        cartography.sort_key  AS sort_key,
        geometry
    FROM read_parquet(
        's3://overturemaps-us-west-2/release/' ||
        getvariable('overture_release') ||
        '/theme=base/type=land_cover/*',
        filename = true,
        hive_partitioning = 1
    )
    WHERE
        bbox.xmin < $extent_xmax AND bbox.xmax > $extent_xmin
        AND bbox.ymin < $extent_ymax AND bbox.ymax > $extent_ymin
) TO '{{overture_data_dir}}/land_cover.parquet'
WITH (FORMAT PARQUET, COMPRESSION 'ZSTD');

-- break

----------------------------------------------------------------------
-- 7. INFRASTRUCTURE
----------------------------------------------------------------------

COPY (
    SELECT
        id,
        subtype,
        class,
        height,
        surface,
        names,
        geometry
    FROM read_parquet(
        's3://overturemaps-us-west-2/release/' ||
        getvariable('overture_release') ||
        '/theme=base/type=infrastructure/*',
        filename = true,
        hive_partitioning = 1
    )
    WHERE
        bbox.xmin < $extent_xmax AND bbox.xmax > $extent_xmin
        AND bbox.ymin < $extent_ymax AND bbox.ymax > $extent_ymin
) TO '{{overture_data_dir}}/infrastructure.parquet'
WITH (FORMAT PARQUET, COMPRESSION 'ZSTD');

-- break
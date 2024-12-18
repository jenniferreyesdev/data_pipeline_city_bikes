
  create view "citybikes"."citybikes"."stg_coverage_analysis__dbt_tmp"
    
    
  as (
    WITH source AS (
    SELECT *
    FROM "citybikes"."citybikes"."coverage_analysis"
)

SELECT
    analysis_id,
    total_stations,
    station_density,
    service_area_sqmi,
    coverage_gaps,
    recommendations,
    CURRENT_TIMESTAMP AS transformation_updated_at
FROM source
  );
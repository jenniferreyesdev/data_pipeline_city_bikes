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
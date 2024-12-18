WITH stations AS (
    SELECT *
    FROM "citybikes"."citybikes"."stg_stations"
),

calculations AS (
    SELECT
        station_id,
        station_name,  -- Changed from name to station_name to match stg_stations
        free_bikes,
        empty_slots,
        (free_bikes + empty_slots) AS total_capacity,
        CASE
            WHEN (free_bikes + empty_slots) > 0 
            THEN ROUND((empty_slots::FLOAT / (free_bikes + empty_slots) * 100)::numeric, 2)
            ELSE 0
        END AS utilization_rate,
        latitude,
        longitude,
        _extracted_at as timestamp,  -- Changed to match stg_stations
        address,  -- This column might need to be removed if not in stg_stations
        CURRENT_TIMESTAMP AS transformation_updated_at
    FROM stations
)

SELECT
    *,
    CASE
        WHEN utilization_rate BETWEEN 90 AND 100 THEN 'Nearly Empty'
        WHEN utilization_rate BETWEEN 75 AND 89 THEN 'High Usage'
        WHEN utilization_rate BETWEEN 40 AND 74 THEN 'Balanced'
        WHEN utilization_rate BETWEEN 25 AND 39 THEN 'Underutilized'
        WHEN utilization_rate BETWEEN 0 AND 24 THEN 'Oversupplied'

    END AS utilization_category
FROM calculations
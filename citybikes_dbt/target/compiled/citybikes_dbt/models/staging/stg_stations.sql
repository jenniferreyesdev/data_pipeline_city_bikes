

SELECT
    station_id,
    name as station_name,
    latitude::float as latitude,
    longitude::float as longitude,
    free_bikes::bigint as free_bikes,
    empty_slots::bigint as empty_slots,
    (free_bikes::bigint + empty_slots::bigint) as total_capacity,
    timestamp as _extracted_at,
    _dlt_id as network_id,
    address  -- Added this line
FROM "citybikes"."citybikes"."station_data"
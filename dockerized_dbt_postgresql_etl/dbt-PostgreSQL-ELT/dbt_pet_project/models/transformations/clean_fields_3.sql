SELECT
    event_id,
    event_source,
    pet_id,
    date,
    NULLIF(issue, '') AS issue,
    NULLIF(resolution, '') AS resolution,
    activity_type,
    CASE 
        WHEN event_source = 'health' THEN 0
        WHEN TRIM(duration_minutes) IN ('-', '') THEN NULL
        ELSE CAST(duration_minutes AS INTEGER)
    END AS duration_minutes
FROM {{ ref('fill_nulls_and_replace_2') }}

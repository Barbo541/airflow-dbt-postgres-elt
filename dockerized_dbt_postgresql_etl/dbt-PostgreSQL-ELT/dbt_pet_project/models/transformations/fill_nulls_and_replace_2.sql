SELECT
    event_id,
    event_source,
    pet_id,
    date,
    issue,
    resolution,
    CASE
      WHEN event_source = 'health' THEN 'Health'
      WHEN TRIM(activity_type) = 'Walk' THEN 'Walking'
      WHEN TRIM(activity_type) = 'Play' THEN 'Playing'
      WHEN TRIM(activity_type) = 'Rest' THEN 'Resting'
      ELSE NULLIF(TRIM(activity_type), '')
    END AS activity_type,
    duration_minutes
FROM {{ ref('combine_health_and_act_1') }}

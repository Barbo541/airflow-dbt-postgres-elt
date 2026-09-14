select *
from {{ ref('final_join_w_users_4') }}
where trim(activity_type) = ''
   or duration_minutes < 0
   or (event_source = 'health' and
       (duration_minutes is distinct from 0 or activity_type <> 'Health'))
   or (event_source = 'activity' and
       (activity_type = 'Health' or issue is not null or resolution is not null))
   or trim(owner_age_group) = '' or trim(pet_type) = ''

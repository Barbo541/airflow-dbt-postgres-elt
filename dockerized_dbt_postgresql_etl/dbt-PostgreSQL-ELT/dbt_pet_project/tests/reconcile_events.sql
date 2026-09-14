-- Counts plus per-event equality catch loss, fanout and provenance mistakes.
with source_counts as (
    select 'health' as event_source, count(*) as n from {{ source('raw', 'raw_pet_health') }}
    union all
    select 'activity', count(*) from {{ source('raw', 'raw_pet_activity') }}
), combined_counts as (
    select event_source, count(*) as n from {{ ref('combine_health_and_act_1') }}
    group by event_source
)
select coalesce(s.event_source, c.event_source) as failure
from source_counts s full join combined_counts c using (event_source)
where s.n is distinct from c.n
union all
select 'intermediate_count'
where (select count(*) from {{ ref('combine_health_and_act_1') }})
    <> (select count(*) from {{ ref('clean_fields_3') }})
union all
select coalesce(c.event_id, f.event_id)
from {{ ref('clean_fields_3') }} c
full join {{ ref('final_join_w_users_4') }} f using (event_id)
group by c.event_id, f.event_id
having count(*) <> 1 or c.event_id is null or f.event_id is null

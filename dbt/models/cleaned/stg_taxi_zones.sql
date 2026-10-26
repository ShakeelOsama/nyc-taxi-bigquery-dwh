select
    location_id,
    nullif(trim(borough), '')                           as borough,
    nullif(trim(zone), '')                              as zone,
    nullif(trim(service_zone), '')                      as service_zone
from {{ source('tlc_raw', 'taxi_zones') }}
where location_id is not null

{{ config(materialized='table') }}

select
    location_id,
    borough,
    zone,
    service_zone,
    case zone
        when 'JFK Airport'       then 'JFK'
        when 'LaGuardia Airport' then 'LGA'
        when 'Newark Airport'    then 'EWR'
    end                                                 as airport_code,
    zone in ('JFK Airport', 'LaGuardia Airport', 'Newark Airport') as is_airport,
    service_zone = 'Yellow Zone'                        as is_manhattan_core,
    location_id in (264, 265)                           as is_unknown
from {{ ref('stg_taxi_zones') }}

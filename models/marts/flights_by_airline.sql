Select airline, count(*) as num_flights
from {{ref('stg_flights_carriers_airports')}}
-- WHERE fl_date = 2019-01-01
GROUP BY airline

WITH aiports as (
        SELECT * from {{ref('dim_airports')}}
    ),
    carriers as (
        SELECT * from {{ref('dim_carriers')}}
    ),
    flights as (
        SELECT * from {{ref('dim_flights')}}
    ),
    merged_carriers as (
        SELECT flights.fl_date, flights.op_unique_carrier, flights.ORIGIN_AIRPORT_ID, flights.DEST_AIRPORT_ID, 
        carriers.description
        from flights
        LEFT JOIN carriers on flights.op_unique_carrier = carriers.code
    ),
    merged_origin as (
        SELECT merged_carriers.fl_date, merged_carriers.op_unique_carrier, merged_carriers.ORIGIN_AIRPORT_ID, 
        merged_carriers.DEST_AIRPORT_ID, merged_carriers.description as airline, aiports.description as origin_airport
        from merged_carriers
        LEFT JOIN aiports on merged_carriers.ORIGIN_AIRPORT_ID = aiports.code
    ),
    merged_arrival as (
        SELECT merged_origin.fl_date, merged_origin.op_unique_carrier, merged_origin.ORIGIN_AIRPORT_ID, 
        merged_origin.DEST_AIRPORT_ID, merged_origin.airline, merged_origin.origin_airport, aiports.description as dest_airport
        from merged_origin
        LEFT JOIN aiports on merged_origin.DEST_AIRPORT_ID = aiports.code
    )
SELECT * from merged_arrival
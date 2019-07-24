class SqlQueries:
    create_staging_listings_table = ("""
         CREATE TABLE IF NOT EXISTS public.staging_listings
         ( 
            listing_id              VARCHAR,         
            minimum_nights          VARCHAR,     
            maximum_nights          VARCHAR,     
            availability_30         VARCHAR,    
            availability_60         VARCHAR,    
            availability_90         VARCHAR,    
            availability_365        VARCHAR,   
            number_of_reviews       VARCHAR,  
            reviews_per_month       VARCHAR,  
            first_review            VARCHAR,       
            last_review             VARCHAR,        
            host_id                 VARCHAR,            
            host_url                VARCHAR,           
            host_name               VARCHAR,          
            host_location           VARCHAR,      
            host_since              VARCHAR,         
            host_listings_count     VARCHAR,
            host_response_rate      VARCHAR, 
            host_response_time      VARCHAR, 
            listing_title           VARCHAR,      
            listing_url             VARCHAR,        
            neighbourhood           VARCHAR,      
            street                  VARCHAR,             
            city                    VARCHAR,               
            state                   VARCHAR,              
            zipcode                 VARCHAR,            
            country                 VARCHAR,            
            property_type           VARCHAR,      
            room_type               VARCHAR,          
            bed_type                VARCHAR,           
            price                   VARCHAR,              
            security_deposit        VARCHAR,   
            cleaning_fee            VARCHAR,
            amenities               VARCHAR(10240),
            accommodates            VARCHAR,       
            bedrooms                VARCHAR,           
            bathrooms               VARCHAR,          
            beds                    VARCHAR,               
            cancellation_policy     VARCHAR 
        );
    """)
    
    create_staging_stays_table = ("""
         CREATE TABLE IF NOT EXISTS public.staging_stays
         ( 
            listing_id      VARCHAR,
            guest_id        VARCHAR,  
            guest_name      VARCHAR,
            stay_id         VARCHAR,   
            stay_date       VARCHAR                     
        );
    """)
    
    create_listings_dim_table = ("""
        CREATE TABLE IF NOT EXISTS public.listings (
            listing_id          VARCHAR,
            listing_url         VARCHAR,
            listing_title       VARCHAR,
            neighbourhood       VARCHAR,
            street              VARCHAR,
            city                VARCHAR,
            zipcode             VARCHAR,
            state               VARCHAR,
            country             VARCHAR,
            amenities           VARCHAR,
            price               REAL,
            bedrooms            REAL,
            bathrooms           REAL,
            cancellation_policy VARCHAR,
            accommodates        REAL,
            beds                REAL,
            bed_type            VARCHAR,
            room_type           VARCHAR,
            property_type       VARCHAR,
            CONSTRAINT listings_pkey PRIMARY KEY (listing_id)
        );
    """)
    
    create_guests_dim_table = ("""
        CREATE TABLE IF NOT EXISTS public.guests (
            guest_id        VARCHAR,
            guest_name      VARCHAR,
            CONSTRAINT guests_pkey PRIMARY KEY (guest_id)
        );
    """)

    create_reviews_dim_table = ("""
        CREATE TABLE IF NOT EXISTS public.reviews (
            listing_id              VARCHAR,
            number_of_reviews       VARCHAR,
            reviews_per_month       VARCHAR,
            first_review            VARCHAR,
            last_review             VARCHAR,
            CONSTRAINT reviews_pkey PRIMARY KEY (listing_id)
        );
    """)

    create_availability_dim_table = ("""
        CREATE TABLE IF NOT EXISTS public.availability (
            listing_id              VARCHAR,
            minimum_nights          INT,
            maximum_nights          INT,
            availability_30         INT,
            availability_60         INT,
            availability_90         INT,
            availability_365        INT,
            CONSTRAINT availability_pkey PRIMARY KEY (listing_id)
        );
    """)

    create_hosts_dim_table = ("""
        CREATE TABLE IF NOT EXISTS public.hosts (
            host_id                 VARCHAR,
            host_url                VARCHAR,
            host_name               VARCHAR,
            host_location           VARCHAR,
            host_since              VARCHAR,
            host_listings_count     VARCHAR,
            host_response_rate      VARCHAR,
            host_response_time      VARCHAR,
            CONSTRAINT hosts_pkey PRIMARY KEY (host_id)
        );
    """)

    create_guest_stays_fact_table = ("""
        CREATE TABLE IF NOT EXISTS public.guest_stays (
            stay_date   VARCHAR,
            stay_id     VARCHAR,
            guest_id    VARCHAR,
            listing_id  VARCHAR,
            host_id     VARCHAR,
            price       REAL,
            CONSTRAINT gueststays_pkey PRIMARY KEY (stay_id)
        );
    """)
        
    listings_dim_insert = ("""
        INSERT INTO listings 
        (   SELECT
                        listing_id,
                        listing_url,
                        listing_title,
                        neighbourhood,
                        street,
                        city,
                        zipcode,
                        state,
                        country,
                        amenities,
                        price::FLOAT4 AS price,
                        bedrooms::FLOAT4 AS bedrooms,
                        bathrooms::FLOAT4 AS bathrooms,
                        cancellation_policy,
                        accommodates::FLOAT4 AS accommodates,
                        beds::FLOAT4 AS beds,
                        bed_type,
                        room_type,
                        property_type
            FROM staging_listings
        )
    """)
    
    guests_dim_insert = ("""
        INSERT INTO guests 
        (   SELECT
                    guest_id,
                    guest_name
            FROM staging_stays
        )
    """)
    
    reviews_dim_insert = ("""
        INSERT INTO reviews 
        (   SELECT
                    listing_id,
                    number_of_reviews,
                    reviews_per_month,
                    first_review,
                    last_review
            FROM staging_listings
        )
    """)
    
    availability_dim_insert = ("""
        INSERT INTO availability 
        (   SELECT
                        listing_id,
                        minimum_nights::INT AS minimum_nights,
                        maximum_nights::INT AS maximum_nights,
                        availability_30::INT AS availability_30,
                        availability_60::INT AS availability_60,
                        availability_90::INT AS availability_90,
                        availability_365::INT AS availability_365
            FROM staging_listings
        )
    """)
    
    hosts_dim_insert = ("""
        INSERT INTO hosts 
        (   SELECT
                        host_id,
                        host_url,
                        host_name,
                        host_location,
                        host_since,
                        host_listings_count,
                        host_response_rate,
                        host_response_time
            FROM staging_listings
        )
    """)
    
    guest_stays_fact_insert = ("""
        INSERT INTO guest_stays 
        (   SELECT
                    stays.stay_date AS stay_date,
                    stays.stay_id AS stay_id,
                    stays.guest_id AS guest_id,
                    listings.listing_id AS listing_id,
                    listings.host_id AS host_id,
                    listings.price::FLOAT4 AS price
            FROM staging_listings listings
            JOIN staging_stays stays
            ON listings.listing_id = stays.listing_id
        )
    """)
    
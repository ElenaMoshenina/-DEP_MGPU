CREATE TABLE orders 
    ( 
     row_id             INTEGER  NOT NULL , 
     order_id           VARCHAR2 
--  ERROR: VARCHAR2 size not specified 
                     NOT NULL , 
     order_date         DATE , 
     ship_date          DATE , 
     ship_mode          VARCHAR2 
--  ERROR: VARCHAR2 size not specified 
                    , 
     customer_id        VARCHAR2 
--  ERROR: VARCHAR2 size not specified 
                    , 
     customer_name      VARCHAR2 
--  ERROR: VARCHAR2 size not specified 
                    , 
     segment            VARCHAR2 
--  ERROR: VARCHAR2 size not specified 
                    , 
     country            VARCHAR2 
--  ERROR: VARCHAR2 size not specified 
                    , 
     city               VARCHAR2 
--  ERROR: VARCHAR2 size not specified 
                    , 
     state              VARCHAR2 
--  ERROR: VARCHAR2 size not specified 
                    , 
     postal_code        VARCHAR2 
--  ERROR: VARCHAR2 size not specified 
                    , 
     region             VARCHAR2 
--  ERROR: VARCHAR2 size not specified 
                    , 
     product_id         VARCHAR2 
--  ERROR: VARCHAR2 size not specified 
                    , 
     category           VARCHAR2 
--  ERROR: VARCHAR2 size not specified 
                    , 
     sub_category       VARCHAR2 
--  ERROR: VARCHAR2 size not specified 
                    , 
     product_name       VARCHAR2 
--  ERROR: VARCHAR2 size not specified 
                    , 
     sales              FLOAT , 
     quantity           INTEGER , 
     discount           FLOAT , 
     profit             FLOAT , 
     returns_returns_ID NUMBER  NOT NULL , 
     data               DATE , 
     people_people_ID   NUMBER  NOT NULL , 
     "date"             DATE 
    ) 
;
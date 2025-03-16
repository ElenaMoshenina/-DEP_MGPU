CREATE TABLE returns 
    ( 
     order_id   VARCHAR2 
--  ERROR: VARCHAR2 size not specified 
                     NOT NULL , 
     returned   CHAR (1)  NOT NULL , 
     returns_ID NUMBER  NOT NULL 
    ) 
;
ALTER TABLE orders 
    ADD CONSTRAINT orders_people_FK FOREIGN KEY 
    ( 
     people_people_ID
    ) 
    REFERENCES people 
    ( 
     people_ID
    ) 
;
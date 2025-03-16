ALTER TABLE orders 
    ADD CONSTRAINT orders_returns_FK FOREIGN KEY 
    ( 
     returns_returns_ID
    ) 
    REFERENCES returns 
    ( 
     returns_ID
    ) 
;
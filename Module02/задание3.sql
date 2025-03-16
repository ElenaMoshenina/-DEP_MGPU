CREATE VIEW dw.avg_category_by AS
SELECT 
    Product_ID,
    Product_Name,
    Discount
FROM 
    orders
ORDER BY 
    Discount DESC
LIMIT 10;
SELECT * FROM dw.avg_category_by
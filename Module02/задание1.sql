CREATE VIEW dw.sales_by_manager AS
SELECT
    customer_name,
    SUM(sales) AS total_sales --общая выручка для каждого способа доставки
FROM
    stg.orders
GROUP BY
    orders.customer_name;

SELECT * FROM dw.sales_by_manager;
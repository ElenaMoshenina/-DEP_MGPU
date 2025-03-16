CREATE VIEW dw.calendar_view AS --представление
SELECT DISTINCT --устранение дубликатов, так как может быть множество заказов у одного клиента
    --выбираем идентификатор, имя, сегмент, географические данные клиента
    Order_date,
	Ship_date
FROM stg.orders;

SELECT * FROM dw.calendar_view;
-- Витрина данных для анализа президентов США
-- Вариант задания №30
-- Создает VIEW на основе обогащенных данных из stg_us_presidents

DROP VIEW IF EXISTS us_presidents_datamart;

CREATE VIEW us_presidents_datamart AS
SELECT 
    president,
    party,
    start_year,
    end_year,
    years_in_office,
    presidency_decade
FROM 
    stg_us_presidents
WHERE
    -- Фильтруем данные, которые могут быть некорректными
    years_in_office >= 0;

-- Комментарий к витрине
COMMENT ON VIEW us_presidents_datamart IS 
'Обогащенная витрина данных для анализа президентов США. Готова для использования в дашбордах.';
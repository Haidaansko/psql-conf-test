SELECT
    month_date, 
    store_id, 
    SUM(units) AS units
FROM
    olap.sales
    NATURAL JOIN olap.menu_items
    NATURAL JOIN olap.stores
    NATURAL JOIN olap.calendar
GROUP BY
    month_date,
    store_id;
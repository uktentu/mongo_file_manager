SELECT *
FROM trm_events
WHERE date >= :start_date AND date <= :end_date;

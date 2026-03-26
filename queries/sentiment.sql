-- sentiment.sql
-- Lee el análisis de sentiment más reciente para cada ticker.
-- Útil para el dashboard: muestra el estado actual del mercado en X.

SELECT
    s.ticker,
    e.nombre,
    e.sector,
    s.fecha,
    s.score,
    s.score_numerico,
    s.menciones,
    s.resumen,
    s.evento
FROM sentiment s
JOIN empresas e ON e.ticker = s.ticker
WHERE (s.ticker, s.fecha) IN (
    SELECT ticker, MAX(fecha)
    FROM sentiment
    GROUP BY ticker
)
ORDER BY s.score_numerico DESC

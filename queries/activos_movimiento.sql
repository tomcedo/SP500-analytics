-- activos_movimiento.sql
-- Top 3 tickers con más menciones en X en su análisis más reciente.
-- Usado en la sección "Activos en movimiento" del Panel General.

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
  AND s.menciones IS NOT NULL
ORDER BY s.menciones DESC
LIMIT 3

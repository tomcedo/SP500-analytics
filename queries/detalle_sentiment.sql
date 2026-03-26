-- detalle_sentiment.sql
-- Análisis de sentiment más reciente para el ticker indicado, con datos de empresa.
-- Parámetro posicional: ticker (?)
-- Usado en la sección de sentiment del Panel Detalle.

SELECT
    s.ticker,
    e.nombre,
    e.sector,
    e.industria,
    s.fecha,
    s.score,
    s.score_numerico,
    s.menciones,
    s.resumen,
    s.evento,
    s.modelo
FROM sentiment s
LEFT JOIN empresas e ON e.ticker = s.ticker
WHERE s.ticker = ?
ORDER BY s.fecha DESC
LIMIT 1

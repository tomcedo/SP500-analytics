-- detalle_precios_15m.sql
-- Precios de 15 minutos (OHLCV) para el ticker indicado.
-- Devuelve los últimos 5 días (de los 60 días disponibles en yfinance).
-- Parámetro posicional: ticker (?)

SELECT
    fecha,
    apertura,
    maximo,
    minimo,
    cierre,
    volumen
FROM precios_15m
WHERE ticker = ?
  AND fecha >= DATETIME('now', '-5 days')
ORDER BY fecha ASC

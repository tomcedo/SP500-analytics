-- detalle_precios_1h.sql
-- Precios horarios (OHLCV) para el ticker indicado.
-- Devuelve los últimos 14 días (de los 60 días disponibles en yfinance).
-- Parámetro posicional: ticker (?)

SELECT
    fecha,
    apertura,
    maximo,
    minimo,
    cierre,
    volumen
FROM precios_1h
WHERE ticker = ?
  AND fecha >= DATETIME('now', '-14 days')
ORDER BY fecha ASC

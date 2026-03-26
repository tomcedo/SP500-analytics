-- detalle_precios.sql
-- OHLCV de los últimos 90 días para el ticker indicado.
-- Parámetro posicional: ticker (?)
-- Usado en el gráfico de velas del Panel Detalle.

SELECT
    fecha,
    apertura,
    maximo,
    minimo,
    cierre,
    volumen
FROM precios
WHERE ticker = ?
  AND fecha >= DATE('now', '-90 days')
ORDER BY fecha ASC

-- detalle_precios_1wk.sql
-- Precios semanales (OHLCV) para el ticker indicado.
-- Devuelve los últimos 2 años de datos semanales.
-- Parámetro posicional: ticker (?)

SELECT
    fecha,
    apertura,
    maximo,
    minimo,
    cierre,
    volumen
FROM precios_1wk
WHERE ticker = ?
  AND fecha >= DATE('now', '-730 days')
ORDER BY fecha ASC

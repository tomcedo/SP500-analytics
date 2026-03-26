-- indicadores.sql
-- Lee todos los precios historicos ordenados por ticker y fecha ascendente.
-- El orden ASC es obligatorio para que pandas-ta calcule series temporales correctamente.

SELECT
    ticker,
    fecha,
    apertura,
    cierre,
    maximo,
    minimo,
    volumen
FROM precios
ORDER BY ticker, fecha ASC

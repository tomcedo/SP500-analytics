-- noticias.sql
-- Noticias más recientes para el ticker indicado, ordenadas de más nueva a más antigua.
-- Parámetro posicional: ticker (?)
-- Usado en la sección de noticias del Panel Detalle.

SELECT
    titulo,
    fuente,
    fecha,
    url,
    descripcion
FROM noticias
WHERE ticker = ?
ORDER BY fecha DESC
LIMIT 4

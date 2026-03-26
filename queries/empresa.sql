-- empresa.sql
-- Metadatos de la empresa para el ticker indicado.
-- Parámetro posicional: ticker (?)
-- Usado en el encabezado del Panel Detalle.

SELECT
    nombre,
    sector,
    industria
FROM empresas
WHERE ticker = ?

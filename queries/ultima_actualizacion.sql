-- ultima_actualizacion.sql
-- Fecha/datetime del último registro disponible para un ticker en una tabla de precios.
-- El nombre de la tabla ({tabla}) se sustituye en Python antes de ejecutar.
-- Parámetro posicional: ticker (?)

SELECT MAX(fecha) AS ultima
FROM {tabla}
WHERE ticker = ?

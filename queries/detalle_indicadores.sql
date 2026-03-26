-- detalle_indicadores.sql
-- Indicadores técnicos de los últimos 90 días para el ticker indicado.
-- Parámetro posicional: ticker (?)
-- Usado en los gráficos RSI, MACD y Bollinger del Panel Detalle.

SELECT
    fecha,
    rsi,
    macd,
    macd_signal,
    macd_hist,
    bb_upper,
    bb_middle,
    bb_lower,
    ema_20,
    ema_50,
    ema_200,
    senal_rsi,
    senal_macd,
    senal_bb,
    senal_tendencia
FROM indicadores
WHERE ticker = ?
  AND fecha >= DATE('now', '-90 days')
ORDER BY fecha ASC

-- score_mid.sql
-- Calcula el Score MID para cada ticker combinando tres señales independientes:
--   RSI:       +20 sobrevendido (<30), -20 sobrecomprado (>70), 0 neutral
--   EMA 200:   +20 precio sobre media de largo plazo, -20 bajo, 0 sin dato
--   Sentiment: +20 positivo (>0.3), -20 negativo (<-0.3), 0 neutral
-- Rango final: -60 a +60. COALESCE garantiza 0 cuando falta algún dato.

WITH ultimo_indicador AS (
    -- RSI y EMA 200 del día más reciente por ticker
    SELECT ticker, rsi, ema_200
    FROM indicadores
    WHERE (ticker, fecha) IN (
        SELECT ticker, MAX(fecha) FROM indicadores GROUP BY ticker
    )
),
ultimo_precio AS (
    -- Precio de cierre más reciente por ticker
    SELECT ticker, cierre
    FROM precios
    WHERE (ticker, fecha) IN (
        SELECT ticker, MAX(fecha) FROM precios GROUP BY ticker
    )
),
ultimo_sentiment AS (
    -- Score numérico del análisis de sentiment más reciente por ticker
    SELECT ticker, score_numerico
    FROM sentiment
    WHERE (ticker, fecha) IN (
        SELECT ticker, MAX(fecha) FROM sentiment GROUP BY ticker
    )
)
SELECT
    e.ticker,
    -- Componente RSI: sobrevendido es oportunidad, sobrecomprado es riesgo
    COALESCE(
        CASE WHEN i.rsi < 30 THEN  20
             WHEN i.rsi > 70 THEN -20
             ELSE 0 END,
        0
    )
    -- Componente tendencia: sobre EMA 200 es señal alcista
    + COALESCE(
        CASE WHEN p.cierre > i.ema_200 THEN  20
             WHEN p.cierre < i.ema_200 THEN -20
             ELSE 0 END,
        0
    )
    -- Componente sentiment: actividad positiva en X suma, negativa resta
    + COALESCE(
        CASE WHEN s.score_numerico >  0.3 THEN  20
             WHEN s.score_numerico < -0.3 THEN -20
             ELSE 0 END,
        0
    )   AS score_mid
FROM empresas e
LEFT JOIN ultimo_indicador  i ON i.ticker = e.ticker
LEFT JOIN ultimo_precio     p ON p.ticker = e.ticker
LEFT JOIN ultimo_sentiment  s ON s.ticker = e.ticker
ORDER BY score_mid DESC

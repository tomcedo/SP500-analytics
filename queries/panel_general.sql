-- panel_general.sql
-- Vista consolidada de los 20 tickers: precio actual, variación diaria,
-- indicadores técnicos y sentiment más recientes.
-- Sirve como fuente de datos de la tabla principal del dashboard.

WITH precio_con_lag AS (
    -- Calcula el precio de cierre anterior usando ventana LAG por ticker
    SELECT
        ticker,
        fecha,
        cierre,
        apertura,
        volumen,
        LAG(cierre) OVER (PARTITION BY ticker ORDER BY fecha) AS cierre_anterior
    FROM precios
),
precio_hoy AS (
    -- Conserva solo la fila más reciente por ticker junto con el lag calculado
    SELECT
        p.ticker,
        p.fecha AS fecha_precio,
        p.cierre,
        p.apertura,
        p.volumen,
        ROUND(
            (p.cierre - p.cierre_anterior) / p.cierre_anterior * 100, 2
        ) AS variacion_pct
    FROM precio_con_lag p
    WHERE (p.ticker, p.fecha) IN (
        SELECT ticker, MAX(fecha) FROM precios GROUP BY ticker
    )
)
SELECT
    e.ticker,
    e.nombre,
    e.sector,
    ph.fecha_precio,
    ph.cierre,
    ph.variacion_pct,
    ph.volumen,
    i.rsi,
    i.senal_rsi,
    i.senal_macd,
    i.senal_bb,
    i.senal_tendencia,
    s.score          AS sentiment_score,
    s.score_numerico AS sentiment_num,
    s.menciones      AS sentiment_menciones
FROM empresas e
LEFT JOIN precio_hoy ph
    ON ph.ticker = e.ticker
LEFT JOIN indicadores i
    ON i.ticker = e.ticker
    AND i.fecha = (SELECT MAX(fecha) FROM indicadores WHERE ticker = e.ticker)
LEFT JOIN sentiment s
    ON s.ticker = e.ticker
    AND s.fecha = (SELECT MAX(fecha) FROM sentiment WHERE ticker = e.ticker)
ORDER BY e.ticker

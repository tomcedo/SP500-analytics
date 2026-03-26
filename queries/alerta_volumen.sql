-- alerta_volumen.sql
-- Detecta actividad de volumen inusual comparando el último día
-- con el promedio de los 20 días anteriores por ticker.
-- Categorías: compra_institucional, venta_institucional, volumen_elevado, ''

WITH vol_historico AS (
    -- Enumerar filas por ticker de más reciente a más antigua
    SELECT
        ticker,
        volumen,
        ROW_NUMBER() OVER (PARTITION BY ticker ORDER BY fecha DESC) AS rn
    FROM precios
),
vol_prom AS (
    -- Promedio de los últimos 20 días (excluye el día actual, rn=1)
    SELECT ticker, AVG(volumen) AS vol_avg_20
    FROM vol_historico
    WHERE rn BETWEEN 2 AND 21
    GROUP BY ticker
),
vol_hoy AS (
    -- Volumen y dirección del precio del último día disponible
    SELECT ticker, volumen AS vol_hoy, cierre, apertura
    FROM precios
    WHERE (ticker, fecha) IN (
        SELECT ticker, MAX(fecha) FROM precios GROUP BY ticker
    )
)
SELECT
    vh.ticker,
    ROUND(vh.vol_hoy * 1.0 / NULLIF(vp.vol_avg_20, 0), 2) AS ratio_vol,
    CASE
        WHEN vh.vol_hoy > 2.0 * vp.vol_avg_20 AND vh.cierre >= vh.apertura
            THEN 'compra_institucional'
        WHEN vh.vol_hoy > 2.0 * vp.vol_avg_20 AND vh.cierre < vh.apertura
            THEN 'venta_institucional'
        WHEN vh.vol_hoy > 1.5 * vp.vol_avg_20
            THEN 'volumen_elevado'
        ELSE ''
    END AS alerta_vol
FROM vol_hoy vh
LEFT JOIN vol_prom vp ON vp.ticker = vh.ticker

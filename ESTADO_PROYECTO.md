# Estado del Proyecto — Market Intelligence Dashboard

Dashboard de análisis técnico, sentiment y noticias para 20 acciones del S&P 500.
Construido con Python + SQLite + Streamlit + Plotly.

---

## Lo que está construido y funcionando

### Pipeline de datos
- [x] Descarga de 2 años de precios OHLCV vía yfinance (20 tickers)
- [x] Base de datos SQLite local con 5 tablas relacionadas
- [x] Cálculo de indicadores técnicos: RSI, MACD, Bollinger Bands, EMA 20/50/200
- [x] Señales accionables generadas automáticamente para cada indicador
- [x] Análisis de sentiment desde X/Twitter vía xAI (Grok 4) con búsqueda en vivo
- [x] Descarga de noticias recientes vía NewsAPI (hasta 4 por ticker)

### Dashboard (app.py)
- [x] Panel General con tabla de los 20 tickers
  - Precio, variación del día, RSI, señal RSI
  - Tendencia EMA 200 (▲ Alcista / ▼ Bajista)
  - Sentiment de X con score numérico
  - Alerta de volumen inusual (compra/venta institucional, volumen elevado)
  - Score MID (−60 a +60) combinando RSI + EMA 200 + Sentiment
- [x] Panel Detalle por ticker (clic en fila → detalle)
  - Métricas de cabecera: precio, volumen, RSI, tendencia
  - Gráfico de velas con BB rellenas + EMA 20/50/200 (rangebreaks para compactar)
  - Gráfico RSI con zonas sobrecompra/sobreventa
  - Gráfico MACD con histograma coloreado
  - Tabla de señales técnicas (RSI, MACD, BB, EMA 200/50/20, comparativa de volumen)
  - Tab Sentiment: métricas, resumen, menciones, botón de actualización live
  - Tab Noticias: últimas 4 noticias con título-link, fuente, fecha, descripción

---

## Archivos del proyecto

```
sp500-analytics/
├── app.py                  Dashboard Streamlit principal
├── etl.py                  Descarga precios históricos y carga la BD
├── technical.py            Calcula indicadores técnicos (EMA, RSI, MACD, BB)
├── sentiment.py            Análisis de sentiment vía xAI/Grok con búsqueda en X
├── news.py                 Descarga noticias desde NewsAPI
├── verificar.py            Diagnóstico: estadísticas de las tablas de la BD
├── requirements.txt        Dependencias Python
├── CLAUDE.md               Convenciones del proyecto para Claude
├── .env.example            Plantilla de variables de entorno
├── .gitignore              Exclusiones git (BD, credenciales, cache)
│
├── data/
│   └── market.db           Base de datos SQLite (NO versionada en git)
│
└── queries/                Todas las consultas SQL del proyecto
    ├── indicadores.sql         Precios para calcular indicadores (leído por technical.py)
    ├── panel_general.sql       Vista consolidada para la tabla principal
    ├── score_mid.sql           Fórmula Score MID (RSI + EMA 200 + Sentiment)
    ├── alerta_volumen.sql      Ratio volumen actual vs promedio 20 días
    ├── activos_movimiento.sql  Top 3 tickers por menciones en X
    ├── empresa.sql             Metadatos de una empresa (nombre, sector, industria)
    ├── detalle_precios.sql     OHLCV últimos 90 días para gráfico de velas
    ├── detalle_indicadores.sql Indicadores técnicos últimos 90 días
    ├── detalle_sentiment.sql   Análisis de sentiment más reciente
    └── noticias.sql            Últimas 4 noticias por ticker
```

### Esquema de la base de datos

| Tabla | Contenido | Cargada por |
|-------|-----------|-------------|
| `empresas` | Ticker, nombre, sector, industria (20 filas fijas) | `etl.py` |
| `precios` | OHLCV diario, ~500 filas × ticker | `etl.py` |
| `indicadores` | RSI, MACD, BB, EMA 20/50/200, señales | `technical.py` |
| `sentiment` | Score, menciones, resumen, eventos por ticker | `sentiment.py` |
| `noticias` | Título, fuente, fecha, URL, descripción | `news.py` |

---

## Variables de entorno requeridas

Copiar `.env.example` como `.env` y completar:

```bash
XAI_API_KEY=...     # API de xAI/Grok — https://console.x.ai
NEWS_API_KEY=...    # NewsAPI — https://newsapi.org/register
```

yfinance no requiere clave de API.

---

## Cómo correr cada script

### Instalación inicial
```bash
pip install -r requirements.txt
```

### 1. ETL — descargar precios históricos
```bash
python etl.py                    # todos los tickers (2 años)
python etl.py --ticker AAPL      # solo un ticker
python etl.py --reset            # borra y recrea las tablas
```
Duración aprox.: 2–3 min para los 20 tickers.

### 2. Indicadores técnicos
```bash
python technical.py              # todos los tickers
python technical.py --ticker AAPL
python technical.py --reset      # necesario si cambiaron columnas de la BD
```

### 3. Sentiment (requiere XAI_API_KEY)
```bash
python sentiment.py              # todos los tickers (~30s por ticker)
python sentiment.py --ticker AAPL
python sentiment.py --reset
```
Límite de rate: Grok puede tardar 20–40s por ticker.

### 4. Noticias (requiere NEWS_API_KEY)
```bash
python news.py                   # todos los tickers
python news.py --ticker AAPL
python news.py --reset
```
Límite: NewsAPI free = 100 requests/día. Hay pausa de 0.5s entre tickers.

### 5. Dashboard
```bash
streamlit run app.py
```
Abre en http://localhost:8501

### 6. Diagnóstico de la BD
```bash
python verificar.py
```
Muestra conteo de registros, rango de fechas y últimas señales por tabla.

### Flujo completo (primera vez)
```bash
python etl.py
python technical.py
python sentiment.py
python news.py
streamlit run app.py
```

### Actualización diaria recomendada
```bash
python etl.py          # nuevos precios
python technical.py    # recalcular indicadores
python news.py         # noticias del día
# sentiment.py según cuota disponible de xAI
```

---

## Pasos pendientes

### Funcionalidad
- [ ] **Actualización automática de precios** — scheduler o cron job que corra `etl.py` + `technical.py` cada día hábil a las 17:00 ET (cierre de mercado NY)
- [ ] **Más tickers** — ampliar de 20 a los 50 o 100 principales del S&P 500 (ajustar lista en `etl.py` y `news.py`)
- [ ] **Alertas por email/Telegram** — notificar cuando un ticker supere umbrales de RSI o Score MID
- [ ] **Histórico de sentiment** — mostrar evolución del score en el tiempo (actualmente solo muestra el más reciente)
- [ ] **Exportar a CSV** — botón en el panel general para descargar la tabla completa

### Técnico
- [ ] **`requirements.txt` con versiones fijas** — agregar `pip freeze > requirements.txt` para reproducibilidad exacta
- [ ] **Tests** — cubrir al menos `calcular_para_ticker()` y las funciones de señal en `technical.py`
- [ ] **Manejo de mercado cerrado** — etl.py no distingue si yfinance devuelve datos del día anterior por ser fin de semana o feriado

---

*Última actualización: 2026-03-26*

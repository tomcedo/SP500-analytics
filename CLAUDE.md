# Market Intelligence Dashboard

Dashboard de análisis técnico y sentiment para acciones del S&P 500.

## Stack

- **Python** — lógica principal
- **SQLite** — almacenamiento local de precios e indicadores
- **SQL** — toda consulta de datos pasa obligatoriamente por archivos `.sql`
- **Streamlit + Plotly** — visualización interactiva
- **yfinance** — descarga de precios históricos
- **pandas-ta** — cálculo de indicadores técnicos
- **xAI API (Grok)** — análisis de sentiment desde X/Twitter

## Estructura del proyecto

```
sp500-analytics/
├── data/           # Base de datos SQLite
├── queries/        # Archivos .sql con todas las consultas
├── app.py          # Dashboard Streamlit principal
├── etl.py          # Descarga datos históricos y carga la BD
├── sentiment.py    # Análisis de sentiment via xAI/Grok
└── technical.py    # Cálculo de indicadores técnicos
```

## Convenciones

- **Idioma de comentarios:** español
- **Nombres de archivos y variables:** snake_case
- **Consultas de datos:** siempre a través de archivos `.sql` en `queries/` — nunca SQL inline en Python
- **Credenciales:** exclusivamente desde variables de entorno, nunca hardcodeadas en el código

## Indicadores técnicos implementados

Cada indicador debe tener un comentario breve y accionable (qué señal genera y cuándo actuar).

| Indicador | Señal orientativa |
|-----------|-------------------|
| RSI | Sobrecompra >70, sobreventa <30 |
| MACD | Cruce de línea señal indica cambio de momentum |
| Bollinger Bands | Precio fuera de banda sugiere reversión o breakout |
| SMA 20 | Tendencia de corto plazo |
| SMA 50 | Tendencia de mediano plazo |
| SMA 200 | Tendencia de largo plazo; golden/death cross con SMA 50 |

## Variables de entorno requeridas

```bash
XAI_API_KEY=...       # Clave de la API de xAI/Grok
NEWS_API_KEY=...      # Clave de NewsAPI (newsapi.org)
```

Cargar con `python-dotenv` en desarrollo; en producción usar el entorno del sistema.

## Reglas para Claude

- No hardcodear credenciales ni tokens bajo ninguna circunstancia.
- Respetar la separación de responsabilidades: ETL en `etl.py`, indicadores en `technical.py`, sentiment en `sentiment.py`, visualización en `app.py`.
- Todo acceso a datos debe pasar por una consulta SQL ubicada en `queries/`.
- Los comentarios en el código deben estar en español y ser concisos.
- Al agregar un indicador técnico, incluir siempre un comentario con la interpretación accionable.
- Preferir editar archivos existentes antes de crear nuevos.
- No agregar dependencias que no estén en el stack definido sin consultar primero.

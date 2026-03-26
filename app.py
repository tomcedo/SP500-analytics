"""
app.py — Dashboard principal del Market Intelligence Dashboard.

Paneles:
    Panel General  — tabla resumen de los 20 tickers + activos en movimiento en X
    Panel Detalle  — gráficos técnicos, señales y sentiment de un ticker

Navegación: session_state.ticker_sel == None → Panel General
            session_state.ticker_sel == "AAPL" → Panel Detalle
"""

import os
import sqlite3
import subprocess
import sys
from pathlib import Path

import pandas as pd
import plotly.graph_objects as go
import streamlit as st
from dotenv import load_dotenv
from plotly.subplots import make_subplots

# ---------------------------------------------------------------------------
# Credenciales — .env en local, st.secrets en Streamlit Cloud
# ---------------------------------------------------------------------------
load_dotenv()  # no-op si no existe .env

# Propagar st.secrets al entorno para que los subprocesos hereden las claves
# (Streamlit Cloud inyecta secrets como env vars, pero load_dotenv los sobreescribe
#  solo si .env existe; este bloque garantiza paridad en ambos entornos)
for _k in ("XAI_API_KEY", "NEWS_API_KEY"):
    if _k not in os.environ:
        try:
            os.environ[_k] = st.secrets[_k]
        except (KeyError, FileNotFoundError):
            pass  # clave no configurada — los scripts que la necesiten lo reportarán

# ---------------------------------------------------------------------------
# Configuración de página — debe ser la primera llamada a Streamlit
# ---------------------------------------------------------------------------
st.set_page_config(
    page_title="Market Intelligence Dashboard",
    page_icon="📈",
    layout="wide",
    initial_sidebar_state="collapsed",
)

# ---------------------------------------------------------------------------
# Constantes
# ---------------------------------------------------------------------------
APP_DIR  = Path(__file__).parent
DB_PATH  = APP_DIR / "data" / "market.db"
SQL_DIR  = APP_DIR / "queries"

# Colores del sistema de diseño (usados en todos los gráficos)
C_VERDE  = "#26a69a"   # alcista, positivo, sobreventa
C_ROJO   = "#ef5350"   # bajista, negativo, sobrecompra
C_AZUL   = "#3b82f6"   # EMA 50, MACD
C_AMBAR  = "#f59e0b"   # EMA 20, RSI
C_VIOLETA= "#a855f7"   # EMA 200
C_GRIS   = "#b0bec5"   # referencias, bandas neutras
TEMPLATE = "plotly_white"

EMOJI_SENT = {"positivo": "🟢", "negativo": "🔴", "neutral": "🟡"}

# ---------------------------------------------------------------------------
# Inicialización automática de BD (Streamlit Cloud no tiene market.db)
# ---------------------------------------------------------------------------

def _bd_necesita_inicializacion() -> bool:
    """True si la BD no existe o la tabla precios está vacía."""
    if not DB_PATH.exists():
        return True
    try:
        conn = sqlite3.connect(DB_PATH)
        count = conn.execute("SELECT COUNT(*) FROM precios").fetchone()[0]
        conn.close()
        return count == 0
    except Exception:
        return True


def _inicializar_bd() -> None:
    """
    Ejecuta etl.py y technical.py como subprocesos para crear y poblar la BD.
    Se muestra solo en el primer arranque (BD ausente o vacía).
    """
    st.info(
        "**Primera ejecución detectada.** "
        "Descargando precios históricos e indicadores técnicos (~2 min)..."
    )
    barra = st.progress(0, text="Paso 1/2 — Descargando precios (etl.py)...")

    res_etl = subprocess.run(
        [sys.executable, "etl.py"],
        capture_output=True, text=True, encoding="utf-8",
        timeout=360, cwd=APP_DIR,
    )
    if res_etl.returncode != 0:
        st.error(f"Error al descargar precios:\n```\n{res_etl.stderr[:800]}\n```")
        st.stop()

    barra.progress(60, text="Paso 2/2 — Calculando indicadores técnicos...")

    res_tech = subprocess.run(
        [sys.executable, "technical.py"],
        capture_output=True, text=True, encoding="utf-8",
        timeout=120, cwd=APP_DIR,
    )
    if res_tech.returncode != 0:
        st.error(f"Error al calcular indicadores:\n```\n{res_tech.stderr[:800]}\n```")
        st.stop()

    # Crear tabla sentiment vacía si no existe — sentiment.py requiere XAI_API_KEY
    # y puede no ejecutarse en cloud; las queries usan LEFT JOIN y toleran tabla vacía
    conn_init = sqlite3.connect(DB_PATH)
    conn_init.execute("""
        CREATE TABLE IF NOT EXISTS sentiment (
            ticker          TEXT    NOT NULL,
            fecha           DATE    NOT NULL,
            score           TEXT,
            score_numerico  REAL,
            menciones       INTEGER,
            resumen         TEXT,
            evento          TEXT,
            modelo          TEXT,
            fecha_carga     DATE,
            UNIQUE (ticker, fecha),
            FOREIGN KEY (ticker) REFERENCES empresas(ticker)
        )
    """)
    conn_init.execute("""
        CREATE TABLE IF NOT EXISTS noticias (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            ticker      TEXT    NOT NULL,
            titulo      TEXT,
            fuente      TEXT,
            fecha       TEXT,
            url         TEXT,
            descripcion TEXT,
            fecha_carga DATE    NOT NULL,
            UNIQUE (ticker, url),
            FOREIGN KEY (ticker) REFERENCES empresas(ticker)
        )
    """)
    conn_init.commit()
    conn_init.close()

    barra.progress(100, text="¡Listo!")
    st.success("Base de datos inicializada correctamente.")
    st.rerun()


# ---------------------------------------------------------------------------
# Utilidades de base de datos
# ---------------------------------------------------------------------------

def _leer_sql(nombre: str) -> str:
    """Lee el contenido de queries/{nombre}.sql."""
    return (SQL_DIR / f"{nombre}.sql").read_text(encoding="utf-8")


def _conectar() -> sqlite3.Connection:
    """Abre conexión a la BD (sin check_same_thread para Streamlit)."""
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


def _query(nombre_sql: str, params: tuple = ()) -> pd.DataFrame:
    """Ejecuta una consulta SQL y devuelve DataFrame. Cierra la conexión."""
    conn = _conectar()
    df   = pd.read_sql_query(_leer_sql(nombre_sql), conn, params=params)
    conn.close()
    return df


# ---------------------------------------------------------------------------
# Carga de datos — cacheada 5 minutos para no saturar la BD en reruns
# ---------------------------------------------------------------------------

@st.cache_data(ttl=300)
def cargar_panel_general() -> pd.DataFrame:
    """Precio, variación, indicadores y sentiment más recientes de los 20 tickers."""
    return _query("panel_general")


@st.cache_data(ttl=300)
def cargar_activos_movimiento() -> pd.DataFrame:
    """Top 3 tickers por menciones en X con su evento principal."""
    return _query("activos_movimiento")


@st.cache_data(ttl=300)
def cargar_empresa(ticker: str) -> pd.DataFrame:
    """Nombre, sector e industria de un ticker."""
    return _query("empresa", params=(ticker,))


@st.cache_data(ttl=300)
def cargar_detalle_precios(ticker: str) -> pd.DataFrame:
    """OHLCV de los últimos 90 días para el ticker."""
    return _query("detalle_precios", params=(ticker,))


@st.cache_data(ttl=300)
def cargar_detalle_indicadores(ticker: str) -> pd.DataFrame:
    """Todos los indicadores técnicos de los últimos 90 días para el ticker."""
    return _query("detalle_indicadores", params=(ticker,))


@st.cache_data(ttl=300)
def cargar_detalle_sentiment(ticker: str) -> pd.DataFrame:
    """Análisis de sentiment más reciente para el ticker."""
    return _query("detalle_sentiment", params=(ticker,))


@st.cache_data(ttl=300)
def cargar_score_mid() -> pd.DataFrame:
    """Score MID calculado via SQL para todos los tickers (-60 a +60)."""
    return _query("score_mid")


@st.cache_data(ttl=300)
def cargar_alerta_volumen() -> pd.DataFrame:
    """Ratio de volumen actual vs promedio 20 días y categoría de alerta."""
    return _query("alerta_volumen")


@st.cache_data(ttl=1800)
def cargar_noticias(ticker: str) -> pd.DataFrame:
    """Últimas 4 noticias del ticker desde la BD (cache 30 min)."""
    return _query("noticias", params=(ticker,))


# ---------------------------------------------------------------------------
# Gráficos
# ---------------------------------------------------------------------------

def grafico_precio_bb(df_p: pd.DataFrame, df_i: pd.DataFrame) -> go.Figure:
    """
    Candlestick + Bollinger Bands rellenas + SMA 20/50/200 + volumen en subplot.
    Las BB se agregan antes del candlestick para que queden detrás.
    """
    fig = make_subplots(
        rows=2, cols=1,
        shared_xaxes=True,
        row_heights=[0.76, 0.24],
        vertical_spacing=0.03,
    )

    # -- Bollinger Bands: upper primero, lower con fill="tonexty" --
    if df_i["bb_upper"].notna().any():
        fig.add_trace(go.Scatter(
            x=df_i["fecha"], y=df_i["bb_upper"],
            name="BB",
            line=dict(color=C_GRIS, width=1, dash="dot"),
            showlegend=True,
        ), row=1, col=1)
        fig.add_trace(go.Scatter(
            x=df_i["fecha"], y=df_i["bb_lower"],
            name="BB",
            line=dict(color=C_GRIS, width=1, dash="dot"),
            fill="tonexty",                            # rellena entre lower y upper
            fillcolor="rgba(176,190,197,0.13)",
            showlegend=False,
        ), row=1, col=1)
        fig.add_trace(go.Scatter(
            x=df_i["fecha"], y=df_i["bb_middle"],
            name="BB Media",
            line=dict(color=C_GRIS, width=1, dash="dash"),
            showlegend=False,
        ), row=1, col=1)

    # -- EMAs --
    for col_name, color, label in [
        ("ema_20",  C_AMBAR,   "EMA 20"),
        ("ema_50",  C_AZUL,    "EMA 50"),
        ("ema_200", C_VIOLETA, "EMA 200"),
    ]:
        if col_name in df_i.columns and df_i[col_name].notna().any():
            fig.add_trace(go.Scatter(
                x=df_i["fecha"], y=df_i[col_name],
                name=label,
                line=dict(color=color, width=1.4),
            ), row=1, col=1)

    # -- Candlestick (encima de todo) --
    fig.add_trace(go.Candlestick(
        x=df_p["fecha"],
        open=df_p["apertura"],
        high=df_p["maximo"],
        low=df_p["minimo"],
        close=df_p["cierre"],
        name="Precio",
        increasing_line_color=C_VERDE,
        decreasing_line_color=C_ROJO,
        showlegend=False,
    ), row=1, col=1)

    # -- Volumen coloreado por dirección --
    colores_vol = [
        C_VERDE if c >= a else C_ROJO
        for c, a in zip(df_p["cierre"], df_p["apertura"])
    ]
    fig.add_trace(go.Bar(
        x=df_p["fecha"], y=df_p["volumen"],
        name="Volumen",
        marker_color=colores_vol,
        opacity=0.7,
        showlegend=False,
    ), row=2, col=1)

    fig.update_layout(
        height=480,
        template=TEMPLATE,
        xaxis_rangeslider_visible=False,
        margin=dict(l=0, r=0, t=10, b=0),
        legend=dict(orientation="h", y=1.06, x=0, font_size=11),
        hovermode="x unified",
    )
    # rangebreaks elimina los gaps de fines de semana sin distorsionar el ancho de las velas
    fig.update_xaxes(
        rangebreaks=[dict(bounds=["sat", "mon"])],
        tickangle=-30,
        nticks=16,
    )
    fig.update_yaxes(title_text="Precio (USD)", row=1, col=1, tickprefix="$")
    fig.update_yaxes(title_text="Volumen", row=2, col=1)
    return fig


def grafico_rsi(df_i: pd.DataFrame) -> go.Figure:
    """
    RSI(14) con bandas de sobrecompra (70) y sobreventa (30).
    Zona 30-70 con relleno suave. Línea punteada en el nivel neutro (50).
    """
    fig = go.Figure()

    # Zona neutral sombreada
    fig.add_hrect(y0=30, y1=70, fillcolor="rgba(176,190,197,0.10)", line_width=0)

    # Líneas de referencia
    for nivel, color, texto, pos in [
        (70, C_ROJO,  "Sobrecompra 70", "top right"),
        (30, C_VERDE, "Sobreventa 30",  "bottom right"),
        (50, C_GRIS,  "",               "top right"),
    ]:
        fig.add_hline(
            y=nivel,
            line_dash="dash" if nivel != 50 else "dot",
            line_color=color, line_width=1,
            annotation_text=texto,
            annotation_position=pos,
            annotation_font_size=10,
        )

    fig.add_trace(go.Scatter(
        x=df_i["fecha"], y=df_i["rsi"],
        name="RSI(14)",
        line=dict(color=C_AMBAR, width=2),
        fill="none",
    ))

    fig.update_layout(
        height=240,
        template=TEMPLATE,
        margin=dict(l=0, r=0, t=10, b=0),
        showlegend=False,
        hovermode="x unified",
        yaxis=dict(range=[0, 100], title="RSI", tickvals=[0, 30, 50, 70, 100]),
    )
    return fig


def grafico_macd(df_i: pd.DataFrame) -> go.Figure:
    """
    MACD(12-26-9): histograma verde/rojo + línea MACD + línea señal.
    El cruce de las dos líneas es la señal accionable principal de momentum.
    """
    hist  = df_i["macd_hist"].fillna(0)
    col_h = [C_VERDE if v >= 0 else C_ROJO for v in hist]

    fig = go.Figure()

    fig.add_trace(go.Bar(
        x=df_i["fecha"], y=hist,
        name="Histograma",
        marker_color=col_h,
        opacity=0.65,
    ))
    fig.add_trace(go.Scatter(
        x=df_i["fecha"], y=df_i["macd"],
        name="MACD",
        line=dict(color=C_VERDE, width=1.6),
    ))
    fig.add_trace(go.Scatter(
        x=df_i["fecha"], y=df_i["macd_signal"],
        name="Señal",
        line=dict(color=C_ROJO, width=1.6),
    ))
    fig.add_hline(y=0, line_color=C_GRIS, line_width=1)

    fig.update_layout(
        height=240,
        template=TEMPLATE,
        margin=dict(l=0, r=0, t=10, b=0),
        barmode="relative",
        hovermode="x unified",
        legend=dict(orientation="h", y=1.12, x=0, font_size=11),
        yaxis_title="MACD",
    )
    return fig


# ---------------------------------------------------------------------------
# Panel General
# ---------------------------------------------------------------------------

def _formatear_tabla(df: pd.DataFrame) -> pd.DataFrame:
    """Prepara columnas de display para la tabla principal."""
    out = df[["ticker", "nombre", "sector", "cierre", "variacion_pct",
              "rsi", "senal_rsi", "senal_tendencia",
              "sentiment_score", "sentiment_num",
              "score_mid", "alerta_vol"]].copy()

    # Variación con indicador direccional
    out["var_fmt"] = out["variacion_pct"].apply(
        lambda x: f"▲ +{x:.2f}%" if pd.notna(x) and x > 0
              else f"▼ {x:.2f}%"  if pd.notna(x) and x < 0
              else "—"
    )
    # Sentiment con emoji
    out["sent_fmt"] = out.apply(
        lambda r: f"{EMOJI_SENT.get(r['sentiment_score'], '⚪')} {r['sentiment_num']:+.2f}"
                  if pd.notna(r["sentiment_score"]) and pd.notna(r["sentiment_num"]) else "N/A",
        axis=1,
    )
    # Tendencia acortada
    out["tend_fmt"] = out["senal_tendencia"].apply(
        lambda x: "▲ Alcista" if pd.notna(x) and "alcista" in str(x).lower()
             else "▼ Bajista" if pd.notna(x) and "bajista" in str(x).lower()
             else "—"
    )
    return out


def _formatear_alerta_vol(alerta) -> str:
    """Formatea la alerta de volumen inusual con emoji."""
    if alerta == "compra_institucional":
        return "🟢 Compra institucional"
    if alerta == "venta_institucional":
        return "🔴 Venta institucional"
    if alerta == "volumen_elevado":
        return "🟡 Volumen elevado"
    return "⚪ Volumen normal"


def _formatear_score_mid(score) -> str:
    """Formatea el Score MID con emoji de color según rango."""
    if score is None or (isinstance(score, float) and pd.isna(score)):
        return "—"
    s = int(score)
    if s >= 20:
        return f"🟢 {s:+d}"
    if s <= -20:
        return f"🔴 {s:+d}"
    return f"🟡 {s:+d}"


def render_panel_general() -> None:
    """Renderiza la página principal: tabla resumen + activos en movimiento."""

    # Encabezado
    c1, c2 = st.columns([5, 1])
    with c1:
        st.title("📈 Market Intelligence Dashboard")
        st.caption("S&P 500 · Análisis técnico + sentiment de X · Datos cacheados 5 min")
    with c2:
        st.metric("Universo", "20 tickers")

    st.divider()

    # ── Tabla principal ──────────────────────────────────────────────────────
    st.subheader("Resumen del mercado")
    st.caption("Hacé clic en una fila para ver el análisis detallado del ticker.")

    with st.spinner("Cargando datos..."):
        df = cargar_panel_general()

    if df.empty:
        st.warning("Sin datos. Ejecutá `python etl.py` primero.")
        return

    # Agregar Score MID (LEFT merge por ticker para no perder filas)
    df_score = cargar_score_mid()
    df = df.merge(df_score, on="ticker", how="left")

    # Agregar alerta de volumen inusual
    df_alerta = cargar_alerta_volumen()
    df = df.merge(df_alerta[["ticker", "alerta_vol"]], on="ticker", how="left")

    df_fmt = _formatear_tabla(df)
    df_fmt["score_mid_fmt"] = df_fmt["score_mid"].apply(_formatear_score_mid)
    df_fmt["alerta_vol_fmt"] = df_fmt["alerta_vol"].fillna("").apply(_formatear_alerta_vol)

    cols_vis = ["ticker", "nombre", "sector", "cierre", "var_fmt",
                "rsi", "senal_rsi", "tend_fmt", "sent_fmt",
                "alerta_vol_fmt", "score_mid_fmt"]

    evento = st.dataframe(
        df_fmt[cols_vis],
        use_container_width=True,
        hide_index=True,
        on_select="rerun",
        selection_mode="single-row",
        key="tabla_general",
        column_config={
            "ticker":        st.column_config.TextColumn("Ticker",          width="small"),
            "nombre":        st.column_config.TextColumn("Empresa",         width="medium"),
            "sector":        st.column_config.TextColumn("Sector",          width="medium"),
            "cierre":        st.column_config.NumberColumn("Precio",        format="$%.2f", width="small"),
            "var_fmt":       st.column_config.TextColumn("Var. día",        width="small"),
            "rsi":           st.column_config.NumberColumn("RSI",           format="%.1f",  width="small"),
            "senal_rsi":     st.column_config.TextColumn("Señal RSI",       width="large"),
            "tend_fmt":      st.column_config.TextColumn("EMA 200",         width="medium"),
            "sent_fmt":      st.column_config.TextColumn("Sentiment",       width="small"),
            "alerta_vol_fmt":st.column_config.TextColumn("Alerta volumen",  width="medium"),
            "score_mid_fmt": st.column_config.TextColumn("Score MID",       width="small"),
        },
    )

    # Navegar al detalle si se seleccionó una fila
    if evento.selection.rows:
        idx = evento.selection.rows[0]
        st.session_state.ticker_sel = df_fmt.iloc[idx]["ticker"]
        st.rerun()  # segunda pasada: ticker_sel ya está seteado → Panel Detalle

    st.divider()

    # ── Activos en movimiento en X ───────────────────────────────────────────
    st.subheader("🔥 Activos en movimiento en X")

    df_mov = cargar_activos_movimiento()

    if df_mov.empty:
        st.info(
            "Sin datos de sentiment disponibles. "
            "Ejecutá `python sentiment.py` para analizar la actividad en X."
        )
        return

    cols = st.columns(len(df_mov))
    for col_w, (_, fila) in zip(cols, df_mov.iterrows()):
        emoji = EMOJI_SENT.get(fila.get("score", ""), "⚪")
        score_num = fila.get("score_numerico") or 0.0
        menciones = int(fila.get("menciones") or 0)
        with col_w:
            st.metric(
                label=f"{emoji} **{fila['ticker']}** — {fila['nombre']}",
                value=f"~{menciones:,} menciones",
                delta=f"{score_num:+.2f} sentiment",
                delta_color="normal",
            )
            evento_txt = str(fila.get("evento") or "").strip()
            resumen_txt = str(fila.get("resumen") or "").strip()
            if evento_txt:
                st.caption(f"📢 {evento_txt[:140]}")
            if resumen_txt:
                with st.expander("Ver resumen"):
                    st.write(resumen_txt)


# ---------------------------------------------------------------------------
# Panel Detalle
# ---------------------------------------------------------------------------

def _tabla_senales(df_i: pd.DataFrame, df_p: pd.DataFrame) -> None:
    """Tabla de señales técnicas accionables de la última fila disponible."""
    ult = df_i.dropna(subset=["senal_rsi"], how="all").tail(1)
    if ult.empty:
        st.info("Sin señales disponibles.")
        return

    row = ult.iloc[0]

    def v(campo, fmt=".2f"):
        val = row.get(campo)
        return f"{val:{fmt}}" if pd.notna(val) else "—"

    # Comparativa de volumen
    vol_actual  = df_p["volumen"].iloc[-1]  if not df_p.empty else None
    vol_prom    = df_p["volumen"].tail(20).mean() if not df_p.empty else None
    if vol_actual is not None and vol_prom and vol_prom > 0:
        ratio = vol_actual / vol_prom
        ratio_txt = f"{ratio:.0%}"
        if ratio > 1.20:
            senal_vol = "Alto — confirma movimiento"
        elif ratio < 0.80:
            senal_vol = "Bajo — movimiento débil"
        else:
            senal_vol = "Normal"
        vol_actual_txt = f"{int(vol_actual):,}"
        vol_prom_txt   = f"{int(vol_prom):,}"
    else:
        ratio_txt      = "—"
        senal_vol      = "—"
        vol_actual_txt = "—"
        vol_prom_txt   = "—"

    senales = pd.DataFrame([
        {"Indicador": "RSI (14)",
         "Valor":     v("rsi", ".1f"),
         "Señal accionable": row.get("senal_rsi") or "—"},
        {"Indicador": "MACD (12-26-9)",
         "Valor":     v("macd", ".4f"),
         "Señal accionable": row.get("senal_macd") or "—"},
        {"Indicador": "Bollinger Bands (20, 2σ)",
         "Valor":     f"[${v('bb_lower')} — ${v('bb_upper')}]",
         "Señal accionable": row.get("senal_bb") or "—"},
        {"Indicador": "Tendencia — EMA 200",
         "Valor":     f"${v('ema_200')}",
         "Señal accionable": row.get("senal_tendencia") or "—"},
        {"Indicador": "EMA 20 (corto plazo)",
         "Valor":     f"${v('ema_20')}",
         "Señal accionable": "Tendencia de corto plazo — reacciona rápido a cambios recientes"},
        {"Indicador": "EMA 50 (mediano plazo)",
         "Valor":     f"${v('ema_50')}",
         "Señal accionable": "Tendencia de mediano plazo — soporte/resistencia dinámica"},
        {"Indicador": "Volumen actual",
         "Valor":     vol_actual_txt,
         "Señal accionable": "Operaciones del último día de mercado"},
        {"Indicador": "Volumen promedio 20 días",
         "Valor":     vol_prom_txt,
         "Señal accionable": "Referencia de actividad normal"},
        {"Indicador": "Relación volumen",
         "Valor":     ratio_txt,
         "Señal accionable": senal_vol},
    ])

    st.dataframe(
        senales,
        use_container_width=True,
        hide_index=True,
        column_config={
            "Indicador":        st.column_config.TextColumn(width="medium"),
            "Valor":            st.column_config.TextColumn(width="small"),
            "Señal accionable": st.column_config.TextColumn(width="large"),
        },
    )


def _seccion_sentiment(ticker: str) -> None:
    """Sección de sentiment con métricas, resumen y botón de actualización."""
    df_s = cargar_detalle_sentiment(ticker)

    col_info, col_btn = st.columns([6, 1])

    with col_btn:
        actualizar = st.button(
            "🔄 Actualizar",
            key="btn_upd_sent",
            help=f"Consultar Grok para ${ticker} vía xAI API (~30s)",
        )

    if actualizar:
        with st.spinner(f"Consultando xAI para ${ticker}..."):
            try:
                res = subprocess.run(
                    [sys.executable, "sentiment.py", "--ticker", ticker],
                    capture_output=True, text=True,
                    encoding="utf-8", timeout=120,
                    cwd=Path(__file__).parent,
                )
            except subprocess.TimeoutExpired:
                st.error("La llamada a xAI superó el límite de 2 minutos.")
                return
        if res.returncode == 0:
            st.success("Sentiment actualizado correctamente.")
            cargar_detalle_sentiment.clear()  # invalida solo esta función
            st.rerun()
        else:
            st.error(f"Error al actualizar:\n{res.stderr[:400]}")
        return

    with col_info:
        if df_s.empty:
            st.info(
                "Sin datos de sentiment para este ticker. "
                "Presioná **Actualizar** para analizar con xAI/Grok."
            )
            return

        row = df_s.iloc[0]
        score     = row.get("score") or "neutral"
        score_num = float(row.get("score_numerico") or 0.0)
        menciones = int(row.get("menciones") or 0)
        resumen   = str(row.get("resumen") or "").strip()
        evento    = str(row.get("evento")  or "").strip()
        fecha_s   = row.get("fecha") or "—"
        modelo    = row.get("modelo") or "—"

        emoji = EMOJI_SENT.get(score, "⚪")

        m1, m2, m3 = st.columns(3)
        m1.metric("Sentiment", f"{emoji} {score.capitalize()}", f"{score_num:+.2f}")
        m2.metric("Menciones aprox.", f"~{menciones:,}")
        m3.metric("Fecha del análisis", fecha_s)

        if resumen:
            st.markdown(f"> {resumen}")
        if evento:
            st.info(f"📢 **Evento destacado:** {evento}")
        st.caption(f"Modelo: `{modelo}`")


def _seccion_noticias(ticker: str) -> None:
    """
    Muestra las 4 noticias más recientes del ticker desde la BD,
    ordenadas de más nueva a más antigua.
    """
    df_n = cargar_noticias(ticker)

    if df_n.empty:
        st.info(
            f"Sin noticias para {ticker}. "
            f"Ejecutá `python news.py --ticker {ticker}` para descargarlas."
        )
        return

    for _, row in df_n.iterrows():
        titulo = row.get("titulo") or "Sin título"
        url    = row.get("url") or ""
        fuente = row.get("fuente") or "Fuente desconocida"
        fecha  = str(row.get("fecha") or "")[:10]
        desc   = row.get("descripcion") or ""

        # Título como hipervínculo si hay URL disponible
        if url:
            st.markdown(f"**[{titulo}]({url})**")
        else:
            st.markdown(f"**{titulo}**")

        c1, c2 = st.columns([1, 3])
        c1.caption(f"📅 {fecha}")
        c2.caption(f"📰 {fuente}")

        if desc:
            st.write(desc)

        st.divider()


def render_panel_detalle(ticker: str) -> None:
    """Renderiza el panel de detalle completo para un ticker."""

    # -- Navegación --
    if st.button("← Volver al panel general"):
        st.session_state.ticker_sel = None
        st.rerun()

    # -- Info de la empresa --
    df_emp = cargar_empresa(ticker)
    if df_emp.empty:
        st.error(f"Ticker '{ticker}' no encontrado en la base de datos.")
        return

    emp = df_emp.iloc[0]
    st.title(f"📊 {ticker} — {emp['nombre']}")
    st.caption(f"{emp['sector']}  ·  {emp['industria']}")
    st.divider()

    # -- Cargar datos de detalle --
    df_p = cargar_detalle_precios(ticker)
    df_i = cargar_detalle_indicadores(ticker)

    if df_p.empty:
        st.error(f"Sin datos de precio para {ticker} en los últimos 90 días.")
        return

    # -- Métricas rápidas --
    precio_hoy   = df_p["cierre"].iloc[-1]
    precio_ayer  = df_p["cierre"].iloc[-2] if len(df_p) > 1 else precio_hoy
    variacion    = (precio_hoy - precio_ayer) / precio_ayer * 100
    vol_hoy      = int(df_p["volumen"].iloc[-1])
    fecha_hoy    = df_p["fecha"].iloc[-1]

    rsi_val = None
    tend_txt = "—"
    if not df_i.empty:
        rsi_serie = df_i["rsi"].dropna()
        if not rsi_serie.empty:
            rsi_val = rsi_serie.iloc[-1]
        ema200_serie = df_i["ema_200"].dropna()
        if not ema200_serie.empty:
            tend_txt = "▲ Alcista" if precio_hoy > ema200_serie.iloc[-1] else "▼ Bajista"

    m1, m2, m3, m4 = st.columns(4)
    m1.metric("Cierre", f"${precio_hoy:,.2f}", f"{variacion:+.2f}%")
    m2.metric("Volumen", f"{vol_hoy:,}")
    m3.metric("RSI (14)", f"{rsi_val:.1f}" if rsi_val is not None else "—")
    m4.metric("Tendencia EMA 200", tend_txt)
    st.caption(f"Datos al {fecha_hoy} · Últimos 90 días")

    st.divider()

    # -- Tabs: Técnico / Sentiment / Noticias --
    tab_tec, tab_sent, tab_news = st.tabs(
        ["📉 Análisis técnico", "💬 Sentiment en X", "📰 Noticias"]
    )

    with tab_tec:
        # Gráfico principal
        if not df_i.empty:
            st.plotly_chart(grafico_precio_bb(df_p, df_i), use_container_width=True)
        else:
            # Sin indicadores: solo candlestick básico
            fig = go.Figure(go.Candlestick(
                x=df_p["fecha"],
                open=df_p["apertura"], high=df_p["maximo"],
                low=df_p["minimo"],   close=df_p["cierre"],
                increasing_line_color=C_VERDE,
                decreasing_line_color=C_ROJO,
            ))
            fig.update_layout(
                height=400, template=TEMPLATE,
                xaxis_rangeslider_visible=False,
            )
            st.plotly_chart(fig, use_container_width=True)
            st.warning("Sin indicadores técnicos. Ejecutá `python technical.py`.")

        # RSI y MACD en dos columnas
        if not df_i.empty:
            c1, c2 = st.columns(2)
            with c1:
                st.markdown("**RSI (14)**")
                if df_i["rsi"].notna().any():
                    st.plotly_chart(grafico_rsi(df_i), use_container_width=True)
                else:
                    st.info("Sin datos de RSI suficientes.")
            with c2:
                st.markdown("**MACD (12-26-9)**")
                if df_i["macd"].notna().any():
                    st.plotly_chart(grafico_macd(df_i), use_container_width=True)
                else:
                    st.info("Sin datos de MACD suficientes.")

        # Tabla de señales
        st.subheader("Señales técnicas accionables")
        if not df_i.empty:
            _tabla_senales(df_i, df_p)

    with tab_sent:
        st.subheader(f"Sentiment de X para ${ticker}")
        _seccion_sentiment(ticker)

    with tab_news:
        st.subheader(f"Noticias recientes — {ticker}")
        _seccion_noticias(ticker)


# ---------------------------------------------------------------------------
# Main — inicialización de session state y routing entre paneles
# ---------------------------------------------------------------------------

# Verificar BD antes de cualquier query — arranca ETL si no existe
if _bd_necesita_inicializacion():
    _inicializar_bd()

if "ticker_sel" not in st.session_state:
    st.session_state.ticker_sel = None

if st.session_state.ticker_sel:
    render_panel_detalle(st.session_state.ticker_sel)
else:
    render_panel_general()

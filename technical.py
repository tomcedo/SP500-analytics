"""
technical.py — Calcula indicadores técnicos sobre los precios históricos
y los guarda en la tabla 'indicadores' de data/market.db.

Indicadores calculados (con pandas/numpy puro, sin pandas-ta):
    RSI(14), MACD(12,26,9), Bollinger Bands(20), EMA 20/50/200

Uso:
    python technical.py                  # calcula todos los tickers
    python technical.py --ticker AAPL    # solo un ticker
    python technical.py --reset          # borra y recrea la tabla antes de calcular
"""

import argparse
import io
import sqlite3
import sys
from pathlib import Path

import numpy as np
import pandas as pd

# ---------------------------------------------------------------------------
# Configuración
# ---------------------------------------------------------------------------

DB_PATH          = Path(__file__).parent / "data" / "market.db"
SQL_INDICADORES  = Path(__file__).parent / "queries" / "indicadores.sql"

# ---------------------------------------------------------------------------
# DDL — SQL de creación de la tabla indicadores (visible como referencia)
# ---------------------------------------------------------------------------
#
# CREATE TABLE IF NOT EXISTS indicadores (
#     id              INTEGER PRIMARY KEY AUTOINCREMENT,
#     ticker          TEXT    NOT NULL,
#     fecha           DATE    NOT NULL,
#     rsi             REAL,           -- RSI de 14 periodos
#     macd            REAL,           -- Linea MACD (12-26)
#     macd_signal     REAL,           -- Linea de señal (EMA 9 del MACD)
#     macd_hist       REAL,           -- Histograma (MACD - señal)
#     bb_upper        REAL,           -- Banda superior de Bollinger (20, 2σ)
#     bb_middle       REAL,           -- Banda media (SMA 20 de las bandas)
#     bb_lower        REAL,           -- Banda inferior de Bollinger (20, 2σ)
#     ema_20          REAL,           -- Media movil exponencial 20 dias
#     ema_50          REAL,           -- Media movil exponencial 50 dias
#     ema_200         REAL,           -- Media movil exponencial 200 dias
#     senal_rsi       TEXT,           -- Señal accionable basada en RSI
#     senal_macd      TEXT,           -- Señal accionable basada en cruce MACD
#     senal_bb        TEXT,           -- Señal accionable basada en Bollinger
#     senal_tendencia TEXT,           -- Señal accionable basada en EMA 200
#     UNIQUE (ticker, fecha),
#     FOREIGN KEY (ticker) REFERENCES empresas(ticker)
# );
#
# CREATE INDEX IF NOT EXISTS idx_ind_ticker_fecha
#     ON indicadores (ticker, fecha DESC);
#
# ---------------------------------------------------------------------------

SQL_CREAR_INDICADORES = """
CREATE TABLE IF NOT EXISTS indicadores (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    ticker          TEXT    NOT NULL,
    fecha           DATE    NOT NULL,
    rsi             REAL,
    macd            REAL,
    macd_signal     REAL,
    macd_hist       REAL,
    bb_upper        REAL,
    bb_middle       REAL,
    bb_lower        REAL,
    ema_20          REAL,
    ema_50          REAL,
    ema_200         REAL,
    senal_rsi       TEXT,
    senal_macd      TEXT,
    senal_bb        TEXT,
    senal_tendencia TEXT,
    UNIQUE (ticker, fecha),
    FOREIGN KEY (ticker) REFERENCES empresas(ticker)
)
"""

SQL_CREAR_INDICE = """
CREATE INDEX IF NOT EXISTS idx_ind_ticker_fecha
    ON indicadores (ticker, fecha DESC)
"""

SQL_UPSERT = """
INSERT INTO indicadores (
    ticker, fecha,
    rsi, macd, macd_signal, macd_hist,
    bb_upper, bb_middle, bb_lower,
    ema_20, ema_50, ema_200,
    senal_rsi, senal_macd, senal_bb, senal_tendencia
)
VALUES (
    :ticker, :fecha,
    :rsi, :macd, :macd_signal, :macd_hist,
    :bb_upper, :bb_middle, :bb_lower,
    :ema_20, :ema_50, :ema_200,
    :senal_rsi, :senal_macd, :senal_bb, :senal_tendencia
)
ON CONFLICT(ticker, fecha) DO UPDATE SET
    rsi             = excluded.rsi,
    macd            = excluded.macd,
    macd_signal     = excluded.macd_signal,
    macd_hist       = excluded.macd_hist,
    bb_upper        = excluded.bb_upper,
    bb_middle       = excluded.bb_middle,
    bb_lower        = excluded.bb_lower,
    ema_20          = excluded.ema_20,
    ema_50          = excluded.ema_50,
    ema_200         = excluded.ema_200,
    senal_rsi       = excluded.senal_rsi,
    senal_macd      = excluded.senal_macd,
    senal_bb        = excluded.senal_bb,
    senal_tendencia = excluded.senal_tendencia
"""


# ---------------------------------------------------------------------------
# Base de datos
# ---------------------------------------------------------------------------

def conectar() -> sqlite3.Connection:
    """Abre la conexión a la base de datos existente."""
    if not DB_PATH.exists():
        raise FileNotFoundError(f"BD no encontrada en {DB_PATH}. Ejecuta etl.py primero.")
    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def crear_tabla_indicadores(conn: sqlite3.Connection, reset: bool = False) -> None:
    """Crea la tabla indicadores y su índice. Con reset=True la elimina primero."""
    if reset:
        print("  [reset] Eliminando tabla indicadores...")
        conn.execute("DROP TABLE IF EXISTS indicadores")

    conn.execute(SQL_CREAR_INDICADORES)
    conn.execute(SQL_CREAR_INDICE)
    conn.commit()
    print("  Tabla lista: indicadores")


# ---------------------------------------------------------------------------
# Lectura de precios
# ---------------------------------------------------------------------------

def leer_precios(conn: sqlite3.Connection, ticker: str | None = None) -> pd.DataFrame:
    """
    Ejecuta queries/indicadores.sql y devuelve todos los precios como DataFrame.
    Si se especifica ticker, filtra solo ese símbolo.
    """
    sql = SQL_INDICADORES.read_text(encoding="utf-8")

    # Filtro opcional por ticker: se inyecta como WHERE antes del ORDER BY
    if ticker:
        sql = sql.replace("FROM precios", f"FROM precios\nWHERE ticker = ?")
        df = pd.read_sql_query(sql, conn, params=(ticker.upper(),))
    else:
        df = pd.read_sql_query(sql, conn)
    return df


# ---------------------------------------------------------------------------
# Cálculo de indicadores (pandas-ta)
# ---------------------------------------------------------------------------

def calcular_para_ticker(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calcula todos los indicadores técnicos usando pandas/numpy puro.
    Sin dependencia de pandas-ta — compatible con Streamlit Cloud.
    Los nombres de columna de salida son idénticos a los que generaba pandas-ta.
    """
    close = df["cierre"]

    # -- EMA 20/50/200 — tendencia de corto, mediano y largo plazo --
    df["EMA_20"]  = close.ewm(span=20,  adjust=False).mean()
    df["EMA_50"]  = close.ewm(span=50,  adjust=False).mean()
    df["EMA_200"] = close.ewm(span=200, adjust=False).mean()

    # -- RSI(14) — Wilder's smoothing (EWM con alpha=1/14) --
    # sobrecompra >70, sobreventa <30
    delta    = close.diff()
    gain     = delta.clip(lower=0)
    loss     = (-delta).clip(lower=0)
    avg_gain = gain.ewm(alpha=1/14, min_periods=14, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1/14, min_periods=14, adjust=False).mean()
    rs = avg_gain / avg_loss.replace(0, np.nan)
    df["RSI_14"] = 100 - (100 / (1 + rs))

    # -- MACD(12,26,9) — cruce de línea señal indica cambio de momentum --
    ema12 = close.ewm(span=12, adjust=False).mean()
    ema26 = close.ewm(span=26, adjust=False).mean()
    df["MACD_12_26_9"]  = ema12 - ema26
    df["MACDs_12_26_9"] = df["MACD_12_26_9"].ewm(span=9, adjust=False).mean()
    df["MACDh_12_26_9"] = df["MACD_12_26_9"] - df["MACDs_12_26_9"]

    # -- Bollinger Bands(20, 2σ) — precio fuera de banda sugiere reversión o breakout --
    bb_media = close.rolling(window=20).mean()
    bb_std   = close.rolling(window=20).std(ddof=1)
    df["BBM_20_2.0"] = bb_media
    df["BBU_20_2.0"] = bb_media + 2 * bb_std
    df["BBL_20_2.0"] = bb_media - 2 * bb_std

    return df


# ---------------------------------------------------------------------------
# Generación de señales accionables
# ---------------------------------------------------------------------------

def _senal_rsi(rsi: float | None) -> str:
    """Interpreta el RSI y devuelve una señal accionable."""
    if rsi is None or np.isnan(rsi):
        return "Sin datos suficientes"
    if rsi > 70:
        return "Sobrecomprado — considerar toma de ganancias"
    if rsi < 30:
        return "Sobrevendido — posible rebote"
    if rsi > 60:
        return "RSI elevado — momentum positivo"
    if rsi < 40:
        return "RSI bajo — momentum negativo"
    return "RSI neutral"


def _senal_macd(macd: float | None, signal: float | None,
                macd_prev: float | None, signal_prev: float | None) -> str:
    """
    Detecta cruce alcista/bajista del MACD con su línea señal.
    Cruce alcista: MACD cruza por encima de la señal → momento positivo.
    Cruce bajista: MACD cruza por debajo → momento negativo.
    """
    vals = [macd, signal, macd_prev, signal_prev]
    if any(v is None or np.isnan(v) for v in vals):
        return "Sin datos suficientes"

    cruce_alcista = (macd_prev <= signal_prev) and (macd > signal)
    cruce_bajista = (macd_prev >= signal_prev) and (macd < signal)

    if cruce_alcista:
        return "Cruce alcista MACD — momento positivo"
    if cruce_bajista:
        return "Cruce bajista MACD — momento negativo"
    if macd > signal:
        return "MACD sobre señal — tendencia positiva"
    return "MACD bajo señal — tendencia negativa"


def _senal_bb(cierre: float | None,
              bb_upper: float | None, bb_lower: float | None) -> str:
    """
    Evalúa la posición del precio respecto a las bandas de Bollinger.
    Fuera de banda sugiere reversión o breakout; dentro es zona neutral.
    """
    if any(v is None or np.isnan(v) for v in [cierre, bb_upper, bb_lower]):
        return "Sin datos suficientes"
    if cierre > bb_upper:
        return "Precio sobre banda superior — posible sobrecompra"
    if cierre < bb_lower:
        return "Precio bajo banda inferior — posible sobreventa"
    return "Precio dentro de bandas Bollinger"


def _senal_tendencia(cierre: float | None, ema_200: float | None) -> str:
    """
    Compara el precio de cierre con la EMA 200.
    Sobre la media indica tendencia alcista de largo plazo; bajo, bajista.
    """
    if any(v is None or np.isnan(v) for v in [cierre, ema_200]):
        return "Sin datos suficientes (menos de 200 dias)"
    if cierre > ema_200:
        return "Tendencia alcista — sobre EMA de largo plazo"
    return "Tendencia bajista — bajo EMA de largo plazo"


def generar_senales(df: pd.DataFrame) -> pd.DataFrame:
    """
    Aplica las cuatro funciones de señal fila por fila y agrega
    las columnas senal_rsi, senal_macd, senal_bb, senal_tendencia.
    """
    # Nombres de columnas generados por pandas-ta
    col_rsi    = "RSI_14"
    col_macd   = "MACD_12_26_9"
    col_signal = "MACDs_12_26_9"
    col_hist   = "MACDh_12_26_9"
    col_bbu    = "BBU_20_2.0"
    col_bbm    = "BBM_20_2.0"
    col_bbl    = "BBL_20_2.0"

    def _nan_to_none(v):
        return None if (v is None or (isinstance(v, float) and np.isnan(v))) else v

    # MACD desplazado un período para detectar cruce
    macd_prev   = df[col_macd].shift(1)
    signal_prev = df[col_signal].shift(1)

    senales_rsi       = []
    senales_macd      = []
    senales_bb        = []
    senales_tendencia = []

    for i in range(len(df)):
        row = df.iloc[i]

        senales_rsi.append(_senal_rsi(
            _nan_to_none(row.get(col_rsi))
        ))
        senales_macd.append(_senal_macd(
            _nan_to_none(row.get(col_macd)),
            _nan_to_none(row.get(col_signal)),
            _nan_to_none(macd_prev.iloc[i]),
            _nan_to_none(signal_prev.iloc[i]),
        ))
        senales_bb.append(_senal_bb(
            _nan_to_none(row.get("cierre")),
            _nan_to_none(row.get(col_bbu)),
            _nan_to_none(row.get(col_bbl)),
        ))
        senales_tendencia.append(_senal_tendencia(
            _nan_to_none(row.get("cierre")),
            _nan_to_none(row.get("EMA_200")),
        ))

    df["senal_rsi"]       = senales_rsi
    df["senal_macd"]      = senales_macd
    df["senal_bb"]        = senales_bb
    df["senal_tendencia"] = senales_tendencia

    return df


# ---------------------------------------------------------------------------
# Guardado en BD
# ---------------------------------------------------------------------------

def _redondear(v, decimales: int = 4):
    """Redondea un valor numérico; devuelve None si es NaN."""
    if v is None:
        return None
    try:
        if np.isnan(float(v)):
            return None
        return round(float(v), decimales)
    except (TypeError, ValueError):
        return None


def cargar_indicadores(conn: sqlite3.Connection, df: pd.DataFrame) -> int:
    """
    Inserta o actualiza los indicadores de un ticker en la tabla indicadores.
    Devuelve la cantidad de filas procesadas.
    """
    col_rsi    = "RSI_14"
    col_macd   = "MACD_12_26_9"
    col_signal = "MACDs_12_26_9"
    col_hist   = "MACDh_12_26_9"
    col_bbu    = "BBU_20_2.0"
    col_bbm    = "BBM_20_2.0"
    col_bbl    = "BBL_20_2.0"

    registros = [
        {
            "ticker":          row["ticker"],
            "fecha":           row["fecha"],
            "rsi":             _redondear(row.get(col_rsi)),
            "macd":            _redondear(row.get(col_macd)),
            "macd_signal":     _redondear(row.get(col_signal)),
            "macd_hist":       _redondear(row.get(col_hist)),
            "bb_upper":        _redondear(row.get(col_bbu)),
            "bb_middle":       _redondear(row.get(col_bbm)),
            "bb_lower":        _redondear(row.get(col_bbl)),
            "ema_20":          _redondear(row.get("EMA_20")),
            "ema_50":          _redondear(row.get("EMA_50")),
            "ema_200":         _redondear(row.get("EMA_200")),
            "senal_rsi":       row.get("senal_rsi"),
            "senal_macd":      row.get("senal_macd"),
            "senal_bb":        row.get("senal_bb"),
            "senal_tendencia": row.get("senal_tendencia"),
        }
        for _, row in df.iterrows()
    ]

    conn.executemany(SQL_UPSERT, registros)
    conn.commit()
    return len(registros)


# ---------------------------------------------------------------------------
# Resumen final
# ---------------------------------------------------------------------------

def mostrar_resumen(conn: sqlite3.Connection) -> None:
    """Imprime estadísticas de la tabla indicadores tras la carga."""
    total = conn.execute("SELECT COUNT(*) FROM indicadores").fetchone()[0]
    tickers = conn.execute("SELECT COUNT(DISTINCT ticker) FROM indicadores").fetchone()[0]

    # Señales del último día disponible por ticker
    ultimas = conn.execute("""
        SELECT ticker, fecha, senal_rsi, senal_tendencia
        FROM indicadores
        WHERE (ticker, fecha) IN (
            SELECT ticker, MAX(fecha) FROM indicadores GROUP BY ticker
        )
        ORDER BY ticker
    """).fetchall()

    print("\n" + "=" * 65)
    print("  Resumen de indicadores")
    print("=" * 65)
    print(f"  Tickers procesados : {tickers}")
    print(f"  Registros totales  : {total:,}")
    print()
    print(f"  {'TICKER':<8}  {'FECHA':<12}  {'SEÑAL RSI':<42}  TENDENCIA")
    print(f"  {'-'*8}  {'-'*12}  {'-'*42}  {'-'*10}")
    for row in ultimas:
        ticker, fecha, s_rsi, s_tend = row
        tend_corta = "ALCISTA" if "alcista" in (s_tend or "").lower() else "BAJISTA"
        print(f"  {ticker:<8}  {fecha:<12}  {(s_rsi or ''):<42}  {tend_corta}")
    print("=" * 65)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    # Forzar UTF-8 en stdout para evitar errores en consolas Windows (cp1252)
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

    parser = argparse.ArgumentParser(
        description="Market Intelligence Dashboard — Indicadores Técnicos"
    )
    parser.add_argument("--ticker", type=str, default=None,
                        help="Calcular solo este ticker (ej: AAPL)")
    parser.add_argument("--reset", action="store_true",
                        help="Eliminar y recrear la tabla indicadores")
    args = parser.parse_args()

    print("\nMarket Intelligence Dashboard — Indicadores Tecnicos")

    conn = conectar()
    crear_tabla_indicadores(conn, reset=args.reset)

    # Leer todos los precios de una vez (más eficiente que N consultas)
    ticker_filtro = args.ticker.upper() if args.ticker else None
    print(f"  Leyendo precios desde BD{' (ticker: ' + ticker_filtro + ')' if ticker_filtro else ''}...")
    df_todos = leer_precios(conn, ticker=ticker_filtro)

    tickers = df_todos["ticker"].unique()
    print(f"  Tickers a procesar: {len(tickers)}\n")

    errores = []
    for ticker in tickers:
        try:
            df_ticker = df_todos[df_todos["ticker"] == ticker].copy().reset_index(drop=True)

            print(f"  [{ticker}] Calculando indicadores ({len(df_ticker)} filas)...", end=" ")

            df_calc   = calcular_para_ticker(df_ticker)
            df_senal  = generar_senales(df_calc)
            insertadas = cargar_indicadores(conn, df_senal)

            # Señal del último día para feedback inmediato
            ultima = df_senal.iloc[-1]
            print(f"OK — {insertadas} registros | "
                  f"RSI={_redondear(ultima.get('RSI_14'), 1)} | "
                  f"{ultima.get('senal_tendencia', '')}")

        except Exception as e:
            print(f"ERROR: {e}")
            errores.append(ticker)

    mostrar_resumen(conn)
    conn.close()

    if errores:
        print(f"\n  Tickers con errores: {', '.join(errores)}")


if __name__ == "__main__":
    main()

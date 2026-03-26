"""
etl.py — Descarga precios históricos del S&P 500 y carga la base de datos SQLite.

Uso:
    python etl.py                          # carga diaria completa (2 años, todos los tickers)
    python etl.py --ticker AAPL            # solo un ticker, intervalo 1d
    python etl.py --interval 1h            # descarga horaria para todos los tickers
    python etl.py --interval all           # todos los intervalos (1d, 1wk, 1h, 15m)
    python etl.py --reset                  # borra y recrea las tablas antes de cargar
"""

import argparse
import sqlite3
import sys
from datetime import datetime, timedelta
from pathlib import Path

import yfinance as yf
import pandas as pd

# ---------------------------------------------------------------------------
# Configuración
# ---------------------------------------------------------------------------

DB_PATH = Path(__file__).parent / "data" / "market.db"

# 20 acciones del S&P 500 seleccionadas
TICKERS = [
    "AAPL", "MSFT", "NVDA", "AMZN", "GOOGL",
    "META", "TSLA", "JPM",  "V",    "XOM",
    "UNH",  "JNJ",  "PG",   "MA",   "HD",
    "BAC",  "ABBV", "CVX",  "MRK",  "LLY",
]

# Período de descarga para intervalos diario/semanal
FECHA_FIN    = datetime.today()
FECHA_INICIO = FECHA_FIN - timedelta(days=365 * 2)

# Configuración de intervalos disponibles
# period=None → usa FECHA_INICIO/FECHA_FIN; period="60d" → últimos 60 días (límite yfinance)
INTERVALOS = {
    "1d":  {"tabla": "precios",     "period": None,  "periodo_desc": "2 años"},
    "1wk": {"tabla": "precios_1wk", "period": None,  "periodo_desc": "2 años"},
    "1h":  {"tabla": "precios_1h",  "period": "60d", "periodo_desc": "60 días"},
    "15m": {"tabla": "precios_15m", "period": "60d", "periodo_desc": "60 días"},
}

# ---------------------------------------------------------------------------
# DDL — SQL de creación de tablas (visible como referencia)
# ---------------------------------------------------------------------------
#
# CREATE TABLE IF NOT EXISTS empresas (
#     ticker    TEXT PRIMARY KEY,
#     nombre    TEXT,
#     sector    TEXT,
#     industria TEXT
# );
#
# CREATE TABLE IF NOT EXISTS precios (
#     id        INTEGER PRIMARY KEY AUTOINCREMENT,
#     ticker    TEXT    NOT NULL,
#     fecha     DATE    NOT NULL,
#     apertura  REAL,
#     cierre    REAL,
#     maximo    REAL,
#     minimo    REAL,
#     volumen   INTEGER,
#     UNIQUE (ticker, fecha),
#     FOREIGN KEY (ticker) REFERENCES empresas(ticker)
# );
#
# CREATE INDEX IF NOT EXISTS idx_precios_ticker_fecha
#     ON precios (ticker, fecha DESC);
#
# ---------------------------------------------------------------------------

SQL_CREAR_EMPRESAS = """
CREATE TABLE IF NOT EXISTS empresas (
    ticker    TEXT PRIMARY KEY,
    nombre    TEXT,
    sector    TEXT,
    industria TEXT
);
"""

SQL_CREAR_PRECIOS = """
CREATE TABLE IF NOT EXISTS precios (
    id        INTEGER PRIMARY KEY AUTOINCREMENT,
    ticker    TEXT    NOT NULL,
    fecha     DATE    NOT NULL,
    apertura  REAL,
    cierre    REAL,
    maximo    REAL,
    minimo    REAL,
    volumen   INTEGER,
    UNIQUE (ticker, fecha),
    FOREIGN KEY (ticker) REFERENCES empresas(ticker)
);
"""

SQL_CREAR_INDICE = """
CREATE INDEX IF NOT EXISTS idx_precios_ticker_fecha
    ON precios (ticker, fecha DESC);
"""

SQL_CREAR_PRECIOS_1WK = """
CREATE TABLE IF NOT EXISTS precios_1wk (
    id        INTEGER PRIMARY KEY AUTOINCREMENT,
    ticker    TEXT    NOT NULL,
    fecha     DATE    NOT NULL,     -- lunes de cada semana (YYYY-MM-DD)
    apertura  REAL,
    cierre    REAL,
    maximo    REAL,
    minimo    REAL,
    volumen   INTEGER,
    UNIQUE (ticker, fecha),
    FOREIGN KEY (ticker) REFERENCES empresas(ticker)
);
"""

SQL_CREAR_PRECIOS_1H = """
CREATE TABLE IF NOT EXISTS precios_1h (
    id        INTEGER PRIMARY KEY AUTOINCREMENT,
    ticker    TEXT    NOT NULL,
    fecha     TEXT    NOT NULL,     -- datetime ET: YYYY-MM-DD HH:MM:SS
    apertura  REAL,
    cierre    REAL,
    maximo    REAL,
    minimo    REAL,
    volumen   INTEGER,
    UNIQUE (ticker, fecha),
    FOREIGN KEY (ticker) REFERENCES empresas(ticker)
);
"""

SQL_CREAR_PRECIOS_15M = """
CREATE TABLE IF NOT EXISTS precios_15m (
    id        INTEGER PRIMARY KEY AUTOINCREMENT,
    ticker    TEXT    NOT NULL,
    fecha     TEXT    NOT NULL,     -- datetime ET: YYYY-MM-DD HH:MM:SS
    apertura  REAL,
    cierre    REAL,
    maximo    REAL,
    minimo    REAL,
    volumen   INTEGER,
    UNIQUE (ticker, fecha),
    FOREIGN KEY (ticker) REFERENCES empresas(ticker)
);
"""


# ---------------------------------------------------------------------------
# Base de datos
# ---------------------------------------------------------------------------

def conectar() -> sqlite3.Connection:
    """Abre (o crea) la base de datos y devuelve la conexión."""
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA journal_mode=WAL")   # escrituras concurrentes más seguras
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def crear_tablas(conn: sqlite3.Connection, reset: bool = False) -> None:
    """Crea todas las tablas de precios. Con reset=True las borra primero."""
    if reset:
        print("  [reset] Eliminando tablas existentes...")
        for tabla in ("precios_15m", "precios_1h", "precios_1wk", "precios", "empresas"):
            conn.execute(f"DROP TABLE IF EXISTS {tabla}")

    conn.execute(SQL_CREAR_EMPRESAS)
    conn.execute(SQL_CREAR_PRECIOS)
    conn.execute(SQL_CREAR_INDICE)
    conn.execute(SQL_CREAR_PRECIOS_1WK)
    conn.execute(SQL_CREAR_PRECIOS_1H)
    conn.execute(SQL_CREAR_PRECIOS_15M)
    conn.commit()
    print("  Tablas listas: empresas, precios, precios_1wk, precios_1h, precios_15m")


# ---------------------------------------------------------------------------
# Descarga y carga de datos
# ---------------------------------------------------------------------------

def obtener_info_empresa(ticker: str) -> dict:
    """
    Descarga metadatos de la empresa desde yfinance.
    Devuelve dict con nombre, sector e industria.
    """
    try:
        info = yf.Ticker(ticker).info
        return {
            "ticker":    ticker,
            "nombre":    info.get("longName", ticker),
            "sector":    info.get("sector", "Desconocido"),
            "industria": info.get("industry", "Desconocido"),
        }
    except Exception as e:
        print(f"    Advertencia: no se pudo obtener info de {ticker}: {e}")
        return {"ticker": ticker, "nombre": ticker, "sector": "N/D", "industria": "N/D"}


def cargar_empresa(conn: sqlite3.Connection, info: dict) -> None:
    """Inserta o actualiza el registro de la empresa (upsert por ticker)."""
    conn.execute(
        """
        INSERT INTO empresas (ticker, nombre, sector, industria)
        VALUES (:ticker, :nombre, :sector, :industria)
        ON CONFLICT(ticker) DO UPDATE SET
            nombre    = excluded.nombre,
            sector    = excluded.sector,
            industria = excluded.industria
        """,
        info,
    )


def descargar_precios(ticker: str, intervalo: str = "1d") -> pd.DataFrame:
    """
    Descarga precios OHLCV desde yfinance para el intervalo indicado.
    Para 1d/1wk usa 2 años de historia; para 1h/15m los últimos 60 días.
    Devuelve DataFrame con columnas normalizadas en español.
    """
    cfg = INTERVALOS[intervalo]

    kwargs: dict = dict(
        tickers=ticker,
        interval=intervalo,
        auto_adjust=True,   # precios ajustados por splits y dividendos
        progress=False,
    )
    if cfg["period"]:
        kwargs["period"] = cfg["period"]
    else:
        kwargs["start"] = FECHA_INICIO.strftime("%Y-%m-%d")
        kwargs["end"]   = FECHA_FIN.strftime("%Y-%m-%d")

    df = yf.download(**kwargs)

    if df.empty:
        return pd.DataFrame()

    # Aplanar MultiIndex si yfinance lo devuelve con múltiples tickers
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = df.columns.get_level_values(0)

    # Convertir a hora del este (ET) y quitar timezone para uniformidad
    if hasattr(df.index, "tz") and df.index.tz is not None:
        df.index = df.index.tz_convert("America/New_York").tz_localize(None)

    df = df.rename(columns={
        "Open":   "apertura",
        "Close":  "cierre",
        "High":   "maximo",
        "Low":    "minimo",
        "Volume": "volumen",
    })

    df["ticker"] = ticker

    # Formato de fecha: incluir hora y minuto para intraday
    if intervalo in ("1h", "15m"):
        df["fecha"] = df.index.strftime("%Y-%m-%d %H:%M:%S")
    else:
        df["fecha"] = df.index.strftime("%Y-%m-%d")

    return df[["ticker", "fecha", "apertura", "cierre", "maximo", "minimo", "volumen"]]


def cargar_precios(conn: sqlite3.Connection, df: pd.DataFrame, tabla: str = "precios") -> int:
    """
    Inserta filas de precios ignorando duplicados (ticker + fecha únicos).
    El parámetro tabla indica en qué tabla de precios insertar.
    Devuelve la cantidad de filas insertadas.
    """
    filas_antes = conn.execute(f"SELECT COUNT(*) FROM {tabla}").fetchone()[0]

    df.to_sql(
        "precios_temp",
        conn,
        if_exists="replace",
        index=False,
        method="multi",
    )
    conn.execute(
        f"""
        INSERT OR IGNORE INTO {tabla} (ticker, fecha, apertura, cierre, maximo, minimo, volumen)
        SELECT ticker, fecha, apertura, cierre, maximo, minimo, volumen
        FROM precios_temp
        """
    )
    conn.execute("DROP TABLE precios_temp")
    conn.commit()

    filas_despues = conn.execute(f"SELECT COUNT(*) FROM {tabla}").fetchone()[0]
    return filas_despues - filas_antes


def procesar_ticker(conn: sqlite3.Connection, ticker: str, intervalo: str = "1d") -> None:
    """Pipeline completo para un ticker: info (solo 1d) → precios → BD."""
    cfg = INTERVALOS[intervalo]

    # Metadatos de empresa solo en carga diaria (idénticos para todos los intervalos)
    if intervalo == "1d":
        print(f"  [{ticker}] Descargando metadatos...")
        info = obtener_info_empresa(ticker)
        cargar_empresa(conn, info)
        nombre = info["nombre"]
    else:
        nombre = ticker

    print(f"  [{ticker}] Descargando {intervalo} ({cfg['periodo_desc']})...")
    df = descargar_precios(ticker, intervalo)

    if df.empty:
        print(f"  [{ticker}] Sin datos. Omitido.")
        return

    tabla = cfg["tabla"]
    insertadas = cargar_precios(conn, df, tabla)
    print(f"  [{ticker}] {len(df)} filas → {insertadas} nuevas en '{tabla}' — {nombre}")


# ---------------------------------------------------------------------------
# Resumen final
# ---------------------------------------------------------------------------

def mostrar_resumen(conn: sqlite3.Connection, intervalos_procesados: list[str]) -> None:
    """Imprime estadísticas de las tablas de precios procesadas."""
    total_empresas = conn.execute("SELECT COUNT(*) FROM empresas").fetchone()[0]

    print("\n" + "=" * 60)
    print("  Resumen de carga")
    print("=" * 60)
    print(f"  Empresas cargadas : {total_empresas}")
    for iv in intervalos_procesados:
        tabla = INTERVALOS[iv]["tabla"]
        try:
            total = conn.execute(f"SELECT COUNT(*) FROM {tabla}").fetchone()[0]
            f_min, f_max = conn.execute(
                f"SELECT MIN(fecha), MAX(fecha) FROM {tabla}"
            ).fetchone()
            print(f"  {tabla:<15}: {total:>8,} filas  ({f_min} → {f_max})")
        except Exception:
            print(f"  {tabla:<15}: (sin datos)")
    print(f"  Base de datos     : {DB_PATH.resolve()}")
    print("=" * 60)


# ---------------------------------------------------------------------------
# Entrada principal
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(description="ETL — Market Intelligence Dashboard")
    parser.add_argument(
        "--ticker",
        type=str,
        default=None,
        help="Procesar solo este ticker (ej: AAPL)",
    )
    parser.add_argument(
        "--interval",
        type=str,
        default="1d",
        choices=["1d", "1wk", "1h", "15m", "all"],
        help="Intervalo de datos (default: 1d). 'all' descarga todos los intervalos.",
    )
    parser.add_argument(
        "--reset",
        action="store_true",
        help="Eliminar y recrear las tablas antes de cargar",
    )
    args = parser.parse_args()

    tickers = [args.ticker.upper()] if args.ticker else TICKERS
    intervalos = list(INTERVALOS.keys()) if args.interval == "all" else [args.interval]

    print(f"\nMarket Intelligence Dashboard — ETL")
    print(f"Tickers   : {', '.join(tickers)}")
    print(f"Intervalos: {', '.join(intervalos)}\n")

    conn = conectar()
    crear_tablas(conn, reset=args.reset)

    errores = []
    for intervalo in intervalos:
        print(f"\n--- Intervalo: {intervalo} ({INTERVALOS[intervalo]['periodo_desc']}) ---")
        for ticker in tickers:
            try:
                procesar_ticker(conn, ticker, intervalo)
            except Exception as e:
                print(f"  [{ticker}] ERROR: {e}")
                errores.append(f"{ticker}/{intervalo}")

    mostrar_resumen(conn, intervalos)
    conn.close()

    if errores:
        print(f"\n  Errores en: {', '.join(errores)}")
        sys.exit(1)


if __name__ == "__main__":
    main()

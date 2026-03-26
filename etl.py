"""
etl.py — Descarga precios históricos del S&P 500 y carga la base de datos SQLite.

Uso:
    python etl.py                  # carga completa (2 años)
    python etl.py --ticker AAPL    # solo un ticker
    python etl.py --reset          # borra y recrea las tablas antes de cargar
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

# Período de descarga
FECHA_FIN   = datetime.today()
FECHA_INICIO = FECHA_FIN - timedelta(days=365 * 2)

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
    """Crea las tablas y el índice. Con reset=True borra todo primero."""
    if reset:
        print("  [reset] Eliminando tablas existentes...")
        conn.execute("DROP TABLE IF EXISTS precios")
        conn.execute("DROP TABLE IF EXISTS empresas")

    conn.execute(SQL_CREAR_EMPRESAS)
    conn.execute(SQL_CREAR_PRECIOS)
    conn.execute(SQL_CREAR_INDICE)
    conn.commit()
    print("  Tablas listas: empresas, precios")


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


def descargar_precios(ticker: str) -> pd.DataFrame:
    """
    Descarga 2 años de precios OHLCV desde yfinance.
    Devuelve DataFrame con columnas normalizadas en español.
    """
    df = yf.download(
        ticker,
        start=FECHA_INICIO.strftime("%Y-%m-%d"),
        end=FECHA_FIN.strftime("%Y-%m-%d"),
        auto_adjust=True,   # precios ajustados por splits y dividendos
        progress=False,
    )

    if df.empty:
        return pd.DataFrame()

    # Aplanar MultiIndex si yfinance lo devuelve con múltiples tickers
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = df.columns.get_level_values(0)

    df = df.rename(columns={
        "Open":   "apertura",
        "Close":  "cierre",
        "High":   "maximo",
        "Low":    "minimo",
        "Volume": "volumen",
    })

    df["ticker"] = ticker
    df["fecha"]  = df.index.strftime("%Y-%m-%d")

    return df[["ticker", "fecha", "apertura", "cierre", "maximo", "minimo", "volumen"]]


def cargar_precios(conn: sqlite3.Connection, df: pd.DataFrame) -> int:
    """
    Inserta filas de precios ignorando duplicados (ticker + fecha únicos).
    Devuelve la cantidad de filas insertadas.
    """
    filas_antes = conn.execute("SELECT COUNT(*) FROM precios").fetchone()[0]

    df.to_sql(
        "precios_temp",
        conn,
        if_exists="replace",
        index=False,
        method="multi",
    )
    conn.execute(
        """
        INSERT OR IGNORE INTO precios (ticker, fecha, apertura, cierre, maximo, minimo, volumen)
        SELECT ticker, fecha, apertura, cierre, maximo, minimo, volumen
        FROM precios_temp
        """
    )
    conn.execute("DROP TABLE precios_temp")
    conn.commit()

    filas_despues = conn.execute("SELECT COUNT(*) FROM precios").fetchone()[0]
    return filas_despues - filas_antes


def procesar_ticker(conn: sqlite3.Connection, ticker: str) -> None:
    """Pipeline completo para un ticker: info → precios → BD."""
    print(f"  [{ticker}] Descargando metadatos...")
    info = obtener_info_empresa(ticker)
    cargar_empresa(conn, info)

    print(f"  [{ticker}] Descargando precios ({FECHA_INICIO.date()} a {FECHA_FIN.date()})...")
    df = descargar_precios(ticker)

    if df.empty:
        print(f"  [{ticker}] Sin datos. Omitido.")
        return

    insertadas = cargar_precios(conn, df)
    print(f"  [{ticker}] {len(df)} filas descargadas, {insertadas} nuevas insertadas — {info['nombre']}")


# ---------------------------------------------------------------------------
# Resumen final
# ---------------------------------------------------------------------------

def mostrar_resumen(conn: sqlite3.Connection) -> None:
    """Imprime estadísticas básicas de lo cargado en la BD."""
    total_precios  = conn.execute("SELECT COUNT(*) FROM precios").fetchone()[0]
    total_empresas = conn.execute("SELECT COUNT(*) FROM empresas").fetchone()[0]
    fecha_min, fecha_max = conn.execute(
        "SELECT MIN(fecha), MAX(fecha) FROM precios"
    ).fetchone()

    print("\n" + "=" * 55)
    print("  Resumen de carga")
    print("=" * 55)
    print(f"  Empresas cargadas : {total_empresas}")
    print(f"  Registros de precio: {total_precios:,}")
    print(f"  Periodo            : {fecha_min} a {fecha_max}")
    print(f"  Base de datos      : {DB_PATH.resolve()}")
    print("=" * 55)


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
        "--reset",
        action="store_true",
        help="Eliminar y recrear las tablas antes de cargar",
    )
    args = parser.parse_args()

    tickers = [args.ticker.upper()] if args.ticker else TICKERS

    print(f"\nMarket Intelligence Dashboard — ETL")
    print(f"Tickers a procesar: {', '.join(tickers)}\n")

    conn = conectar()
    crear_tablas(conn, reset=args.reset)

    errores = []
    for ticker in tickers:
        try:
            procesar_ticker(conn, ticker)
        except Exception as e:
            print(f"  [{ticker}] ERROR: {e}")
            errores.append(ticker)

    mostrar_resumen(conn)
    conn.close()

    if errores:
        print(f"\n  Tickers con errores: {', '.join(errores)}")
        sys.exit(1)


if __name__ == "__main__":
    main()

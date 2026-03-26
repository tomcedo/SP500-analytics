"""
news.py — Descarga las 4 noticias más recientes por ticker via NewsAPI
          y las guarda en la tabla 'noticias' de data/market.db.

Uso:
    python news.py                   # descarga todos los tickers
    python news.py --ticker AAPL     # solo un ticker
    python news.py --reset           # borra y recrea la tabla antes de descargar

Requiere:
    Variable de entorno NEWS_API_KEY con la clave de newsapi.org
    En desarrollo: crear archivo .env con NEWS_API_KEY=...
"""

import argparse
import io
import sqlite3
import sys
import time
from datetime import date
from pathlib import Path

import requests
from dotenv import load_dotenv
import os

# Forzar UTF-8 en stdout para consolas Windows (cp1252)
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

# Cargar variables de entorno desde .env si existe
load_dotenv()

# ---------------------------------------------------------------------------
# Configuración
# ---------------------------------------------------------------------------

DB_PATH          = Path(__file__).parent / "data" / "market.db"
NEWSAPI_URL      = "https://newsapi.org/v2/everything"
ARTICULOS_MAX    = 4       # máximo a guardar por ticker
ARTICULOS_FETCH  = 10      # cuántos pedir (filtrar los mejores 4)
PAUSA_ENTRE_TICKERS = 0.5  # segundos — NewsAPI free: 100 req/día

TICKERS = [
    "AAPL", "MSFT", "NVDA", "AMZN", "GOOGL",
    "META", "TSLA", "JPM",  "V",    "XOM",
    "UNH",  "JNJ",  "PG",   "MA",   "HD",
    "BAC",  "ABBV", "CVX",  "MRK",  "LLY",
]

# ---------------------------------------------------------------------------
# DDL — SQL de creación de la tabla noticias (visible como referencia)
# ---------------------------------------------------------------------------
#
# CREATE TABLE IF NOT EXISTS noticias (
#     id          INTEGER PRIMARY KEY AUTOINCREMENT,
#     ticker      TEXT    NOT NULL,
#     titulo      TEXT,
#     fuente      TEXT,
#     fecha       TEXT,        -- publishedAt[:10] de NewsAPI (YYYY-MM-DD)
#     url         TEXT,
#     descripcion TEXT,
#     fecha_carga DATE    NOT NULL,   -- fecha en que se descargó
#     UNIQUE (ticker, url),
#     FOREIGN KEY (ticker) REFERENCES empresas(ticker)
# );
#
# ---------------------------------------------------------------------------

SQL_CREAR_NOTICIAS = """
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
"""

SQL_INSERT = """
INSERT OR IGNORE INTO noticias
    (ticker, titulo, fuente, fecha, url, descripcion, fecha_carga)
VALUES
    (:ticker, :titulo, :fuente, :fecha, :url, :descripcion, :fecha_carga)
"""


# ---------------------------------------------------------------------------
# Base de datos
# ---------------------------------------------------------------------------

def conectar() -> sqlite3.Connection:
    """Abre la conexión a la base de datos existente."""
    if not DB_PATH.exists():
        raise FileNotFoundError(
            f"BD no encontrada en {DB_PATH}. Ejecutá etl.py primero."
        )
    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def crear_tabla_noticias(conn: sqlite3.Connection, reset: bool = False) -> None:
    """Crea la tabla noticias. Con reset=True la elimina primero."""
    if reset:
        print("  [reset] Eliminando tabla noticias...")
        conn.execute("DROP TABLE IF EXISTS noticias")
    conn.execute(SQL_CREAR_NOTICIAS)
    conn.commit()
    print("  Tabla lista: noticias")


def obtener_empresas(conn: sqlite3.Connection, tickers: list[str]) -> dict[str, str]:
    """Devuelve dict {ticker: nombre} para los tickers indicados."""
    placeholders = ",".join("?" * len(tickers))
    filas = conn.execute(
        f"SELECT ticker, nombre FROM empresas WHERE ticker IN ({placeholders})",
        tickers,
    ).fetchall()
    return {row[0]: row[1] for row in filas}


# ---------------------------------------------------------------------------
# API de NewsAPI
# ---------------------------------------------------------------------------

def obtener_api_key() -> str:
    """
    Lee NEWS_API_KEY desde el entorno.
    Lanza EnvironmentError si no está definida.
    """
    key = os.environ.get("NEWS_API_KEY", "").strip()
    if not key:
        raise EnvironmentError(
            "Variable de entorno NEWS_API_KEY no definida. "
            "Configurala en .env o en el entorno del sistema."
        )
    return key


def buscar_noticias(ticker: str, nombre: str, api_key: str) -> list[dict]:
    """
    Consulta NewsAPI buscando artículos recientes sobre el ticker.
    Descarta artículos eliminados ("[Removed]") y devuelve los más recientes.
    Lanza RuntimeError en caso de error HTTP no recuperable.
    """
    # Búsqueda por ticker y nombre de empresa para mayor cobertura
    nombre_corto = nombre.split(" ")[0]  # "Apple" en lugar de "Apple Inc."
    query = f'"{ticker}" OR "{nombre_corto}"'

    params = {
        "q":        query,
        "language": "en",
        "sortBy":   "publishedAt",
        "pageSize": ARTICULOS_FETCH,
        "apiKey":   api_key,
    }

    try:
        resp = requests.get(NEWSAPI_URL, params=params, timeout=15)
    except requests.exceptions.Timeout:
        raise RuntimeError(f"Timeout al consultar NewsAPI para {ticker}")
    except requests.exceptions.ConnectionError as e:
        raise RuntimeError(f"Error de conexión para {ticker}: {e}")

    # Rate limit — espera según Retry-After si está disponible
    if resp.status_code == 429:
        espera = int(resp.headers.get("Retry-After", 60))
        print(f"    Rate limit (429). Esperando {espera}s...")
        time.sleep(espera)
        resp = requests.get(NEWSAPI_URL, params=params, timeout=15)

    if resp.status_code == 401:
        raise RuntimeError(
            "NEWS_API_KEY inválida. Verificá la clave en newsapi.org."
        )

    resp.raise_for_status()

    articulos_raw = resp.json().get("articles", [])

    # Filtrar artículos eliminados o sin título
    articulos = [
        a for a in articulos_raw
        if a.get("title") and a["title"] != "[Removed]"
        and a.get("url")
    ]

    return articulos


# ---------------------------------------------------------------------------
# Guardado en BD
# ---------------------------------------------------------------------------

def guardar_noticias(
    conn: sqlite3.Connection,
    ticker: str,
    articulos: list[dict],
) -> int:
    """
    Inserta hasta ARTICULOS_MAX noticias en la BD usando INSERT OR IGNORE
    (no duplica si el par ticker+url ya existe).
    Devuelve la cantidad de filas efectivamente insertadas.
    """
    hoy = date.today().isoformat()
    registros = []

    for art in articulos[:ARTICULOS_MAX]:
        fecha_pub = (art.get("publishedAt") or "")[:10]  # solo la parte YYYY-MM-DD
        registros.append({
            "ticker":      ticker,
            "titulo":      (art.get("title") or "")[:500],
            "fuente":      (art.get("source", {}).get("name") or "")[:200],
            "fecha":       fecha_pub,
            "url":         (art.get("url") or "")[:1000],
            "descripcion": (art.get("description") or "")[:1000],
            "fecha_carga": hoy,
        })

    if not registros:
        return 0

    filas_antes = conn.execute(
        "SELECT COUNT(*) FROM noticias WHERE ticker = ?", (ticker,)
    ).fetchone()[0]

    conn.executemany(SQL_INSERT, registros)
    conn.commit()

    filas_despues = conn.execute(
        "SELECT COUNT(*) FROM noticias WHERE ticker = ?", (ticker,)
    ).fetchone()[0]

    return filas_despues - filas_antes


# ---------------------------------------------------------------------------
# Resumen final
# ---------------------------------------------------------------------------

def mostrar_resumen(conn: sqlite3.Connection) -> None:
    """Imprime estadísticas de la tabla noticias tras la carga."""
    total = conn.execute("SELECT COUNT(*) FROM noticias").fetchone()[0]
    tickers_con_news = conn.execute(
        "SELECT COUNT(DISTINCT ticker) FROM noticias"
    ).fetchone()[0]
    fecha_min, fecha_max = conn.execute(
        "SELECT MIN(fecha), MAX(fecha) FROM noticias"
    ).fetchone()

    print("\n" + "=" * 58)
    print("  Resumen de noticias")
    print("=" * 58)
    print(f"  Tickers con noticias : {tickers_con_news}")
    print(f"  Artículos totales    : {total}")
    print(f"  Rango de fechas      : {fecha_min} a {fecha_max}")
    print("=" * 58)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Market Intelligence Dashboard — Noticias via NewsAPI"
    )
    parser.add_argument("--ticker", type=str, default=None,
                        help="Descargar solo este ticker (ej: AAPL)")
    parser.add_argument("--reset", action="store_true",
                        help="Eliminar y recrear la tabla noticias")
    args = parser.parse_args()

    print("\nMarket Intelligence Dashboard — Noticias")

    api_key = obtener_api_key()
    print(f"  API key: ...{api_key[-6:]}")

    conn = conectar()
    crear_tabla_noticias(conn, reset=args.reset)

    tickers_a_procesar = [args.ticker.upper()] if args.ticker else TICKERS
    nombres = obtener_empresas(conn, tickers_a_procesar)
    print(f"  Tickers a procesar: {len(tickers_a_procesar)}\n")

    errores = []
    for i, ticker in enumerate(tickers_a_procesar):
        nombre = nombres.get(ticker, ticker)
        print(f"  [{ticker}] {nombre}...", end=" ", flush=True)

        try:
            articulos = buscar_noticias(ticker, nombre, api_key)
            insertadas = guardar_noticias(conn, ticker, articulos)
            print(f"{len(articulos)} encontradas, {insertadas} nuevas guardadas")
        except Exception as e:
            print(f"ERROR: {e}")
            errores.append(ticker)

        # Pausa entre requests para respetar el rate limit de NewsAPI
        if i < len(tickers_a_procesar) - 1:
            time.sleep(PAUSA_ENTRE_TICKERS)

    mostrar_resumen(conn)
    conn.close()

    if errores:
        print(f"\n  Tickers con errores: {', '.join(errores)}")
        sys.exit(1)


if __name__ == "__main__":
    main()

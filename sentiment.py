"""
sentiment.py — Analiza el sentiment de X (Twitter) para acciones del S&P 500
usando la API de xAI (Grok) con búsqueda en vivo de posts recientes.

Uso:
    python sentiment.py                   # analiza todos los tickers
    python sentiment.py --ticker AAPL     # solo un ticker
    python sentiment.py --dry-run         # muestra el prompt sin llamar a la API
    python sentiment.py --reset           # borra y recrea la tabla antes de analizar

Requiere:
    Variable de entorno XAI_API_KEY con la clave de la API de xAI.
    En desarrollo: crear archivo .env con XAI_API_KEY=...
"""

import argparse
import io
import json
import os
import re
import sqlite3
import sys
import time
from datetime import date
from pathlib import Path

from dotenv import load_dotenv

# Forzar UTF-8 en stdout para consolas Windows (cp1252)
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

# Cargar variables de entorno desde .env si existe
load_dotenv()

# ---------------------------------------------------------------------------
# Configuración
# ---------------------------------------------------------------------------

DB_PATH         = Path(__file__).parent / "data" / "market.db"
SQL_SENTIMENT   = Path(__file__).parent / "queries" / "sentiment.sql"
XAI_API_URL     = "https://api.x.ai/v1/responses"
MODELO          = "grok-4"

# Pausa mínima entre llamadas a la API para respetar rate limits
PAUSA_ENTRE_LLAMADAS  = 3.0   # segundos
MAX_REINTENTOS        = 3
PAUSA_BASE_REINTENTO  = 10.0  # segundos (se duplica en cada reintento)

TICKERS = [
    "AAPL", "MSFT", "NVDA", "AMZN", "GOOGL",
    "META", "TSLA", "JPM",  "V",    "XOM",
    "UNH",  "JNJ",  "PG",   "MA",   "HD",
    "BAC",  "ABBV", "CVX",  "MRK",  "LLY",
]

# ---------------------------------------------------------------------------
# DDL — SQL de creación de la tabla sentiment (visible como referencia)
# ---------------------------------------------------------------------------
#
# CREATE TABLE IF NOT EXISTS sentiment (
#     id              INTEGER PRIMARY KEY AUTOINCREMENT,
#     ticker          TEXT    NOT NULL,
#     fecha           DATE    NOT NULL,
#     score           TEXT    NOT NULL,  -- "positivo" | "negativo" | "neutral"
#     score_numerico  REAL,              -- rango -1.0 (muy negativo) a 1.0 (muy positivo)
#     menciones       INTEGER,           -- cantidad aproximada de posts encontrados
#     resumen         TEXT,              -- síntesis de qué dice la gente (2-3 líneas)
#     evento          TEXT,              -- noticia o evento que genera actividad (puede ser NULL)
#     modelo          TEXT,              -- modelo de xAI usado para el análisis
#     UNIQUE (ticker, fecha),
#     FOREIGN KEY (ticker) REFERENCES empresas(ticker)
# );
#
# ---------------------------------------------------------------------------

SQL_CREAR_SENTIMENT = """
CREATE TABLE IF NOT EXISTS sentiment (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    ticker          TEXT    NOT NULL,
    fecha           DATE    NOT NULL,
    score           TEXT    NOT NULL,
    score_numerico  REAL,
    menciones       INTEGER,
    resumen         TEXT,
    evento          TEXT,
    modelo          TEXT,
    UNIQUE (ticker, fecha),
    FOREIGN KEY (ticker) REFERENCES empresas(ticker)
)
"""

SQL_UPSERT = """
INSERT INTO sentiment (ticker, fecha, score, score_numerico, menciones, resumen, evento, modelo)
VALUES (:ticker, :fecha, :score, :score_numerico, :menciones, :resumen, :evento, :modelo)
ON CONFLICT(ticker, fecha) DO UPDATE SET
    score          = excluded.score,
    score_numerico = excluded.score_numerico,
    menciones      = excluded.menciones,
    resumen        = excluded.resumen,
    evento         = excluded.evento,
    modelo         = excluded.modelo
"""

# ---------------------------------------------------------------------------
# Prompt del sistema — instruye a Grok para análisis estructurado
# ---------------------------------------------------------------------------

SYSTEM_PROMPT = """Eres un analista financiero especializado en sentiment de redes sociales.
Tu tarea es analizar el sentiment actual en X (Twitter) sobre una acción específica del S&P 500.

Usa tu capacidad de búsqueda en vivo en X para encontrar posts recientes sobre el ticker indicado.
Responde ÚNICAMENTE con un objeto JSON válido, sin texto adicional, sin bloques de código markdown.

El JSON debe tener exactamente esta estructura:
{
  "score": "positivo" | "negativo" | "neutral",
  "score_numerico": número entre -1.0 y 1.0,
  "menciones": número entero aproximado de posts encontrados,
  "resumen": "síntesis de 2-3 líneas de qué está diciendo la gente sobre esta acción",
  "evento": "descripción del evento o noticia principal que genera actividad, o null si no hay ninguno relevante"
}

Criterios para score_numerico:
  1.0  = sentimiento extremadamente positivo
  0.5  = sentimiento moderadamente positivo
  0.0  = sentimiento neutral o mixto
 -0.5  = sentimiento moderadamente negativo
 -1.0  = sentimiento extremadamente negativo
"""


def prompt_usuario(ticker: str, nombre: str) -> str:
    """Genera el prompt de usuario para analizar el sentiment de un ticker en X."""
    hoy = date.today().strftime("%Y-%m-%d")
    return (
        f"Analiza el sentiment actual en X (Twitter) sobre la acción ${ticker} ({nombre}) "
        f"a la fecha de hoy {hoy}.\n"
        f"Busca posts recientes con los hashtags y términos: ${ticker}, #{ticker}, {ticker} stock, "
        f"{nombre}.\n"
        f"Considera posts de las últimas 24-48 horas preferentemente.\n"
        f"Responde solo con el JSON estructurado indicado."
    )


# ---------------------------------------------------------------------------
# Base de datos
# ---------------------------------------------------------------------------

def conectar() -> sqlite3.Connection:
    """Abre la conexión a la base de datos existente."""
    if not DB_PATH.exists():
        raise FileNotFoundError(
            f"BD no encontrada en {DB_PATH}. Ejecuta etl.py primero."
        )
    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def crear_tabla_sentiment(conn: sqlite3.Connection, reset: bool = False) -> None:
    """Crea la tabla sentiment. Con reset=True la elimina primero."""
    if reset:
        print("  [reset] Eliminando tabla sentiment...")
        conn.execute("DROP TABLE IF EXISTS sentiment")
    conn.execute(SQL_CREAR_SENTIMENT)
    conn.commit()
    print("  Tabla lista: sentiment")


def obtener_empresas(conn: sqlite3.Connection, tickers: list[str]) -> dict[str, str]:
    """Devuelve dict {ticker: nombre} para los tickers indicados."""
    placeholders = ",".join("?" * len(tickers))
    filas = conn.execute(
        f"SELECT ticker, nombre FROM empresas WHERE ticker IN ({placeholders})",
        tickers,
    ).fetchall()
    return {row[0]: row[1] for row in filas}


def guardar_sentiment(conn: sqlite3.Connection, datos: dict) -> None:
    """Inserta o actualiza el registro de sentiment para un ticker y fecha."""
    conn.execute(SQL_UPSERT, datos)
    conn.commit()


# ---------------------------------------------------------------------------
# Llamada a la API de xAI
# ---------------------------------------------------------------------------

def obtener_api_key():
    from dotenv import load_dotenv
    load_dotenv(override=True)
    key = os.environ.get("XAI_API_KEY", "").strip()
    if not key:
        raise EnvironmentError("Variable de entorno XAI_API_KEY no definida.")
    return key


def llamar_api(ticker: str, nombre: str, api_key: str) -> dict:
    """
    Llama al Responses API de xAI (POST /v1/responses) con búsqueda en vivo en X.
    Incluye reintentos con backoff exponencial ante errores 429 (rate limit).
    Devuelve el JSON parseado de la respuesta del modelo.
    """
    import requests  # importación local para claridad de dependencia
    from datetime import timedelta

    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type":  "application/json",
    }

    # Ventana de búsqueda: últimas 48 horas
    hoy       = date.today()
    hace_2d   = hoy - timedelta(days=2)

    payload = {
        "model": MODELO,
        # Responses API usa "input" en lugar de "messages"
        "input": [
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user",   "content": prompt_usuario(ticker, nombre)},
        ],
        # Búsqueda en X como tool (reemplaza search_parameters del endpoint legacy)
        "tools": [
            {
                "type": "x_search",
                "filters": {
                    "from_date": hace_2d.isoformat(),
                    "to_date":   hoy.isoformat(),
                },
            }
        ],
        "temperature": 0.1,   # temperatura baja para respuestas más consistentes
        "store": False,        # no almacenar en servidores de xAI
    }

    pausa = PAUSA_BASE_REINTENTO
    for intento in range(1, MAX_REINTENTOS + 1):
        try:
            respuesta = requests.post(
                XAI_API_URL,
                headers=headers,
                json=payload,
                timeout=60,
            )

            # Rate limit — esperar y reintentar con backoff exponencial
            if respuesta.status_code == 429:
                retry_after = int(respuesta.headers.get("Retry-After", pausa))
                print(f"    Rate limit (429). Esperando {retry_after}s "
                      f"(intento {intento}/{MAX_REINTENTOS})...")
                time.sleep(retry_after)
                pausa *= 2
                continue

            respuesta.raise_for_status()
            return respuesta.json()

        except requests.exceptions.Timeout:
            print(f"    Timeout en intento {intento}/{MAX_REINTENTOS}...")
            if intento < MAX_REINTENTOS:
                time.sleep(pausa)
                pausa *= 2
            else:
                raise

        except requests.exceptions.HTTPError as e:
            # Errores no recuperables (401, 403, etc.) — no reintentar
            raise RuntimeError(f"Error HTTP {respuesta.status_code}: {e}") from e

    raise RuntimeError(f"Se agotaron {MAX_REINTENTOS} reintentos para {ticker}")


# ---------------------------------------------------------------------------
# Parsing de la respuesta
# ---------------------------------------------------------------------------

def extraer_json(texto: str) -> dict:
    """
    Extrae el objeto JSON de la respuesta del modelo.
    Maneja casos donde el modelo envuelve el JSON en texto o bloques de código.
    """
    # Intento directo
    try:
        return json.loads(texto.strip())
    except json.JSONDecodeError:
        pass

    # Buscar bloque ```json ... ``` o ``` ... ```
    bloque = re.search(r"```(?:json)?\s*(\{.*?\})\s*```", texto, re.DOTALL)
    if bloque:
        try:
            return json.loads(bloque.group(1))
        except json.JSONDecodeError:
            pass

    # Buscar el primer { ... } válido en el texto
    match = re.search(r"\{[^{}]*\}", texto, re.DOTALL)
    if match:
        try:
            return json.loads(match.group())
        except json.JSONDecodeError:
            pass

    raise ValueError(f"No se pudo extraer JSON válido de la respuesta:\n{texto[:300]}")


def parsear_respuesta(respuesta_api: dict) -> dict:
    """
    Extrae el contenido del Responses API y lo parsea como JSON estructurado.
    Estructura de respuesta: output_text (shorthand) o output[N].content[0].text
    Valida y normaliza los campos esperados.
    """
    # Intento 1: campo conveniente output_text presente en la raíz
    contenido = respuesta_api.get("output_text", "")

    # Intento 2: navegar el array output → buscar item tipo "message"
    if not contenido:
        for item in respuesta_api.get("output", []):
            if item.get("type") == "message":
                for bloque in item.get("content", []):
                    if bloque.get("type") == "output_text":
                        contenido = bloque.get("text", "")
                        break
            if contenido:
                break

    if not contenido:
        raise ValueError(
            f"No se encontró contenido en la respuesta:\n"
            f"{json.dumps(respuesta_api, indent=2)[:400]}"
        )

    datos = extraer_json(contenido)

    # Normalizar score textual
    score_raw = str(datos.get("score", "neutral")).lower().strip()
    if score_raw not in ("positivo", "negativo", "neutral"):
        # Mapear variantes en inglés o inesperadas
        mapeo = {
            "positive": "positivo", "negative": "negativo",
            "bullish":  "positivo", "bearish":  "negativo",
            "mixed":    "neutral",  "mixed/neutral": "neutral",
        }
        score_raw = mapeo.get(score_raw, "neutral")

    # Asegurar que score_numerico esté en rango [-1, 1]
    score_num = float(datos.get("score_numerico", 0.0))
    score_num = max(-1.0, min(1.0, score_num))

    # Consistencia entre score textual y numérico
    if score_raw == "positivo" and score_num < 0:
        score_num = abs(score_num)
    elif score_raw == "negativo" and score_num > 0:
        score_num = -abs(score_num)

    return {
        "score":          score_raw,
        "score_numerico": round(score_num, 3),
        "menciones":      int(datos.get("menciones", 0)),
        "resumen":        str(datos.get("resumen", "")).strip()[:1000],
        "evento":         str(datos.get("evento", "") or "").strip()[:500] or None,
    }


# ---------------------------------------------------------------------------
# Pipeline por ticker
# ---------------------------------------------------------------------------

def analizar_ticker(
    ticker: str,
    nombre: str,
    conn: sqlite3.Connection,
    api_key: str,
    dry_run: bool = False,
) -> dict | None:
    """
    Ejecuta el análisis completo para un ticker:
    1. Llama a la API de xAI con búsqueda en X
    2. Parsea la respuesta
    3. Guarda en la BD
    Devuelve el dict con los resultados o None si hubo error.
    """
    if dry_run:
        print(f"\n  [DRY RUN] Prompt para {ticker}:")
        print("  " + "-" * 55)
        print(f"  {prompt_usuario(ticker, nombre)}")
        print("  " + "-" * 55)
        return None

    respuesta_api = llamar_api(ticker, nombre, api_key)
    datos = parsear_respuesta(respuesta_api)

    registro = {
        "ticker":          ticker,
        "fecha":           date.today().isoformat(),
        "modelo":          MODELO,
        **datos,
    }
    guardar_sentiment(conn, registro)
    return registro


# ---------------------------------------------------------------------------
# Resumen final
# ---------------------------------------------------------------------------

def mostrar_resumen(conn: sqlite3.Connection) -> None:
    """Imprime un resumen del sentiment guardado leyendo desde queries/sentiment.sql."""
    sql = SQL_SENTIMENT.read_text(encoding="utf-8")
    filas = conn.execute(sql).fetchall()

    if not filas:
        print("  (sin datos de sentiment)")
        return

    print("\n" + "=" * 75)
    print("  Resumen de sentiment — último análisis por ticker")
    print("=" * 75)
    print(f"  {'TICKER':<7}  {'SCORE':<10}  {'NUM':>6}  {'MENCIONES':>10}  EVENTO")
    print(f"  {'-'*7}  {'-'*10}  {'-'*6}  {'-'*10}  {'-'*30}")

    for fila in filas:
        ticker, nombre, sector, fecha, score, score_num, menciones, resumen, evento = fila
        evento_corto = (evento or "—")[:35]
        score_str = f"{score_num:+.2f}" if score_num is not None else "  N/D"
        print(f"  {ticker:<7}  {score:<10}  {score_str:>6}  {str(menciones or 0):>10}  {evento_corto}")

    positivos = sum(1 for f in filas if f[4] == "positivo")
    negativos = sum(1 for f in filas if f[4] == "negativo")
    neutrales = sum(1 for f in filas if f[4] == "neutral")
    print(f"\n  Positivos: {positivos}  |  Negativos: {negativos}  |  Neutrales: {neutrales}")
    print("=" * 75)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Market Intelligence Dashboard — Sentiment via xAI/Grok"
    )
    parser.add_argument("--ticker",  type=str, default=None,
                        help="Analizar solo este ticker (ej: AAPL)")
    parser.add_argument("--reset",   action="store_true",
                        help="Eliminar y recrear la tabla sentiment antes de analizar")
    parser.add_argument("--dry-run", action="store_true",
                        help="Mostrar prompts sin llamar a la API")
    args = parser.parse_args()

    print("\nMarket Intelligence Dashboard — Sentiment xAI/Grok")

    # Verificar API key antes de conectar a la BD (falla rápido si no está configurada)
    api_key = None
    if not args.dry_run:
        api_key = obtener_api_key()
        print(f"  API key: ...{api_key[-6:]}")

    conn    = conectar()
    crear_tabla_sentiment(conn, reset=args.reset)

    tickers_a_procesar = [args.ticker.upper()] if args.ticker else TICKERS
    nombres = obtener_empresas(conn, tickers_a_procesar)

    print(f"  Tickers a analizar: {len(tickers_a_procesar)}")
    if not args.dry_run:
        print(f"  Modelo: {MODELO}  |  Pausa entre llamadas: {PAUSA_ENTRE_LLAMADAS}s\n")

    errores = []
    for i, ticker in enumerate(tickers_a_procesar):
        nombre = nombres.get(ticker, ticker)
        print(f"  [{ticker}] {nombre}...", end=" ", flush=True)

        try:
            resultado = analizar_ticker(ticker, nombre, conn, api_key, dry_run=args.dry_run)

            if resultado:
                score_str = f"{resultado['score_numerico']:+.2f}"
                print(
                    f"{resultado['score'].upper()} ({score_str}) | "
                    f"~{resultado['menciones']} menciones"
                )

        except Exception as e:
            print(f"ERROR: {e}")
            errores.append(ticker)

        # Pausa entre llamadas para respetar rate limits (excepto en dry-run)
        if not args.dry_run and i < len(tickers_a_procesar) - 1:
            time.sleep(PAUSA_ENTRE_LLAMADAS)

    if not args.dry_run:
        mostrar_resumen(conn)

    conn.close()

    if errores:
        print(f"\n  Tickers con errores: {', '.join(errores)}")
        sys.exit(1)


if __name__ == "__main__":
    main()

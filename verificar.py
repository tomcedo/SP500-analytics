"""
verificar.py — Verifica la integridad y contenido de data/market.db
ejecutando 3 consultas de diagnóstico.
"""

import sqlite3
from pathlib import Path

DB_PATH = Path("data/market.db")


def conectar() -> sqlite3.Connection:
    if not DB_PATH.exists():
        raise FileNotFoundError(f"No se encontró la base de datos en {DB_PATH}. Ejecutá etl.py primero.")
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row   # acceso a columnas por nombre
    return conn


def separador(titulo: str) -> None:
    ancho = 58
    print("\n" + "=" * ancho)
    print(f"  {titulo}")
    print("=" * ancho)


def mostrar_tabla(filas: list, encabezados: list[str]) -> None:
    """Imprime una tabla simple alineada en columnas."""
    if not filas:
        print("  (sin resultados)")
        return

    # Calcular ancho máximo por columna (encabezado o dato)
    anchos = [len(h) for h in encabezados]
    for fila in filas:
        for i, celda in enumerate(fila):
            anchos[i] = max(anchos[i], len(str(celda)))

    fmt = "  " + "  ".join(f"{{:<{a}}}" for a in anchos)
    separador_fila = "  " + "  ".join("-" * a for a in anchos)

    print(fmt.format(*encabezados))
    print(separador_fila)
    for fila in filas:
        print(fmt.format(*[str(c) for c in fila]))


# ---------------------------------------------------------------------------
# Consulta 1 — Registros por ticker
# ---------------------------------------------------------------------------
#
#   SELECT
#       e.ticker,
#       e.nombre,
#       COUNT(p.id)  AS cantidad
#   FROM empresas e
#   LEFT JOIN precios p ON p.ticker = e.ticker
#   GROUP BY e.ticker
#   ORDER BY cantidad DESC;
#
# Cuenta cuántas filas de precios tiene cada ticker usando un LEFT JOIN con
# la tabla empresas para incluir también el nombre de la empresa.
# Se ordena de mayor a menor para detectar rápidamente si algún ticker
# tiene menos datos de lo esperado.
# ---------------------------------------------------------------------------

SQL_REGISTROS_POR_TICKER = """
SELECT
    e.ticker,
    e.nombre,
    COUNT(p.id)  AS cantidad
FROM empresas e
LEFT JOIN precios p ON p.ticker = e.ticker
GROUP BY e.ticker
ORDER BY cantidad DESC
"""

def consulta_registros_por_ticker(conn: sqlite3.Connection) -> None:
    separador("1. Registros por ticker (mayor a menor)")
    print("""
  Cuenta las filas de precios de cada empresa. Un ticker con
  menos registros que el resto puede indicar un problema en la
  descarga o que la accion tiene menos historia disponible.
""")
    print(f"  SQL:\n{SQL_REGISTROS_POR_TICKER}")

    filas = conn.execute(SQL_REGISTROS_POR_TICKER).fetchall()
    mostrar_tabla(filas, ["TICKER", "EMPRESA", "REGISTROS"])
    print(f"\n  Total tickers: {len(filas)}  |  Total filas: {sum(f['cantidad'] for f in filas):,}")


# ---------------------------------------------------------------------------
# Consulta 2 — Máximo y mínimo de cierre en los últimos 30 días
# ---------------------------------------------------------------------------
#
#   SELECT
#       p.ticker,
#       e.nombre,
#       ROUND(MAX(p.cierre), 2)  AS max_cierre,
#       ROUND(MIN(p.cierre), 2)  AS min_cierre,
#       ROUND(MAX(p.cierre) - MIN(p.cierre), 2)        AS rango,
#       ROUND((MAX(p.cierre) - MIN(p.cierre))
#             / MIN(p.cierre) * 100, 1)                AS rango_pct
#   FROM precios p
#   JOIN empresas e ON e.ticker = p.ticker
#   WHERE p.fecha >= DATE('now', '-30 days')
#   GROUP BY p.ticker
#   ORDER BY rango_pct DESC;
#
# Busca el precio de cierre más alto y más bajo de cada acción en los
# últimos 30 días. Agrega el rango absoluto y porcentual para medir
# cuánto se movió el precio: rangos altos indican mayor volatilidad.
# ---------------------------------------------------------------------------

SQL_MAX_MIN_30_DIAS = """
SELECT
    p.ticker,
    e.nombre,
    ROUND(MAX(p.cierre), 2)  AS max_cierre,
    ROUND(MIN(p.cierre), 2)  AS min_cierre,
    ROUND(MAX(p.cierre) - MIN(p.cierre), 2)        AS rango,
    ROUND((MAX(p.cierre) - MIN(p.cierre))
          / MIN(p.cierre) * 100, 1)                AS rango_pct
FROM precios p
JOIN empresas e ON e.ticker = p.ticker
WHERE p.fecha >= DATE('now', '-30 days')
GROUP BY p.ticker
ORDER BY rango_pct DESC
"""

def consulta_max_min_30_dias(conn: sqlite3.Connection) -> None:
    separador("2. Max y min de cierre en los ultimos 30 dias")
    print("""
  Precio de cierre maximo y minimo de cada accion en los ultimos
  30 dias. El rango porcentual muestra la volatilidad relativa:
  un rango alto significa que el precio oscilo mucho en el periodo.
""")
    print(f"  SQL:\n{SQL_MAX_MIN_30_DIAS}")

    filas = conn.execute(SQL_MAX_MIN_30_DIAS).fetchall()
    mostrar_tabla(
        filas,
        ["TICKER", "EMPRESA", "MAX $", "MIN $", "RANGO $", "RANGO %"],
    )


# ---------------------------------------------------------------------------
# Consulta 3 — Ticker con mayor volumen promedio en 2025
# ---------------------------------------------------------------------------
#
#   SELECT
#       p.ticker,
#       e.nombre,
#       e.sector,
#       ROUND(AVG(p.volumen))        AS vol_promedio,
#       ROUND(MAX(p.volumen))        AS vol_maximo,
#       COUNT(p.id)                  AS dias_operados
#   FROM precios p
#   JOIN empresas e ON e.ticker = p.ticker
#   WHERE p.fecha BETWEEN '2025-01-01' AND '2025-12-31'
#   GROUP BY p.ticker
#   ORDER BY vol_promedio DESC
#   LIMIT 10;
#
# Calcula el volumen promedio de cada ticker durante el año 2025,
# mostrando el top 10. Un volumen alto refleja mayor liquidez e
# interés del mercado en esa acción. Se incluye también el volumen
# máximo y los días operados como contexto adicional.
# ---------------------------------------------------------------------------

SQL_MAYOR_VOLUMEN_2025 = """
SELECT
    p.ticker,
    e.nombre,
    e.sector,
    ROUND(AVG(p.volumen))        AS vol_promedio,
    ROUND(MAX(p.volumen))        AS vol_maximo,
    COUNT(p.id)                  AS dias_operados
FROM precios p
JOIN empresas e ON e.ticker = p.ticker
WHERE p.fecha BETWEEN '2025-01-01' AND '2025-12-31'
GROUP BY p.ticker
ORDER BY vol_promedio DESC
LIMIT 10
"""

def consulta_mayor_volumen_2025(conn: sqlite3.Connection) -> None:
    separador("3. Top 10 tickers por volumen promedio en 2025")
    print("""
  Rankea las acciones por su volumen diario promedio durante 2025.
  Mayor volumen = mayor liquidez y actividad del mercado.
  Util para detectar las acciones mas negociadas del universo cargado.
""")
    print(f"  SQL:\n{SQL_MAYOR_VOLUMEN_2025}")

    filas = conn.execute(SQL_MAYOR_VOLUMEN_2025).fetchall()

    # Formatear volúmenes con separador de miles para legibilidad
    filas_fmt = [
        (
            f["ticker"],
            f["nombre"][:28],
            f["sector"][:20],
            f"{int(f['vol_promedio']):,}",
            f"{int(f['vol_maximo']):,}",
            f["dias_operados"],
        )
        for f in filas
    ]
    mostrar_tabla(
        filas_fmt,
        ["TICKER", "EMPRESA", "SECTOR", "VOL PROM", "VOL MAX", "DIAS"],
    )

    if filas:
        ganador = filas[0]
        print(f"\n  Lider: {ganador['ticker']} ({ganador['nombre']}) "
              f"con {int(ganador['vol_promedio']):,} acciones/dia en promedio")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    print("\nMarket Intelligence Dashboard — Verificacion de BD")
    print(f"Base de datos: {DB_PATH.resolve()}")

    conn = conectar()

    consulta_registros_por_ticker(conn)
    consulta_max_min_30_dias(conn)
    consulta_mayor_volumen_2025(conn)

    conn.close()
    print("\n" + "=" * 58)
    print("  Verificacion completada.")
    print("=" * 58 + "\n")


if __name__ == "__main__":
    main()

import sqlite3
import pandas as pd

# Conectar a la base de datos
db_path = r"C:\Users\Usuario\Documents\1. Fundae\_ Busines Analytics\Nivel-4-BigData-ETL-DataMining\Reto-7-ETL-Procesos\03_data_warehouse\tienda_online.db"
conn = sqlite3.connect(db_path)

print("=" * 70)
print("📊 CONSULTA DE BASE DE DATOS - TIENDA ONLINE")
print("=" * 70)

# Ver todas las tablas
print("\n📋 TABLAS DISPONIBLES:")
query_tablas = "SELECT name FROM sqlite_master WHERE type='table'"
tablas = pd.read_sql_query(query_tablas, conn)
print(tablas)

# Ver estructura de ventas_consolidadas
print("\n🔍 ESTRUCTURA DE TABLA: ventas_consolidadas")
query_columnas = "PRAGMA table_info(ventas_consolidadas)"
columnas = pd.read_sql_query(query_columnas, conn)
print(columnas[['name', 'type']])

# Ver primeros registros CON METADATOS
print("\n📦 PRIMEROS 5 REGISTROS (con metadatos):")
query = """
SELECT 
    venta_id,
    producto_id,
    cliente_id,
    total,
    fecha_carga,
    fuente_dato,
    categoria_venta
FROM ventas_consolidadas
LIMIT 5
"""
resultado = pd.read_sql_query(query, conn)
print(resultado.to_string(index=False))

# Verificar que TODOS tienen metadatos
print("\n✅ VERIFICACIÓN DE METADATOS:")
query_verificar = """
SELECT 
    COUNT(*) as total_registros,
    COUNT(fecha_carga) as con_fecha_carga,
    COUNT(fuente_dato) as con_fuente_dato,
    COUNT(categoria_venta) as con_categoria_venta
FROM ventas_consolidadas
"""
verificacion = pd.read_sql_query(query_verificar, conn)
print(verificacion.to_string(index=False))

# Ver valores únicos de fuente_dato
print("\n📌 FUENTES DE DATO:")
query_fuentes = """
SELECT DISTINCT fuente_dato, COUNT(*) as cantidad
FROM ventas_consolidadas
GROUP BY fuente_dato
"""
fuentes = pd.read_sql_query(query_fuentes, conn)
print(fuentes.to_string(index=False))

conn.close()
print("\n" + "=" * 70)
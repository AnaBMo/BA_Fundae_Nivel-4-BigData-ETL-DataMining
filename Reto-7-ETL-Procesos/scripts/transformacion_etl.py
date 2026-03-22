"""
Script de Transformación ETL - Tienda Online
Reto 7: Proceso ETL con Apache NiFi + Python

Este script realiza las transformaciones (T) del proceso ETL:
1. Lee los archivos extraídos por NiFi
2. Transforma y limpia los datos
3. Une (JOIN) los 3 datasets
4. Agrega metadatos
5. Carga el resultado en SQLite
"""

import pandas as pd
import json
import sqlite3
from datetime import datetime
import os

print("="*70)
print("🔄 INICIANDO TRANSFORMACIÓN ETL - TIENDA ONLINE")
print("="*70)

# ===============================================
# 1. CONFIGURACIÓN DE RUTAS
# ===============================================
print("\n📁 Configurando rutas...")

# Ruta base del proyecto
base_path = r"C:\Users\Usuario\Documents\1. Fundae\_ Busines Analytics\Nivel-4-BigData-ETL-DataMining\Reto-7-ETL-Procesos"

# Rutas de entrada (archivos extraídos por NiFi)
path_productos = os.path.join(base_path, "02_datos_procesados_nifi", "productos", "productos.csv")
path_ventas = os.path.join(base_path, "02_datos_procesados_nifi", "ventas", "ventas.csv")
path_clientes = os.path.join(base_path, "02_datos_procesados_nifi", "clientes", "clientes.json")

# Ruta de salida
output_path = os.path.join(base_path, "03_data_warehouse")
db_path = os.path.join(output_path, "tienda_online.db")

print(f"✅ Rutas configuradas")
print(f"   Productos: {path_productos}")
print(f"   Ventas: {path_ventas}")
print(f"   Clientes: {path_clientes}")
print(f"   Base de datos: {db_path}")

# ===============================================
# 2. EXTRACCIÓN - Leer archivos
# ===============================================
print("\n📥 PASO 1: EXTRACCIÓN DE DATOS")
print("-" * 70)

# Leer productos (CSV)
print("📦 Leyendo productos...")
df_productos = pd.read_csv(path_productos)
print(f"   ✅ Productos cargados: {len(df_productos)} registros")
print(f"   Columnas: {list(df_productos.columns)}")

# Leer ventas (CSV)
print("\n💰 Leyendo ventas...")
df_ventas = pd.read_csv(path_ventas)
print(f"   ✅ Ventas cargadas: {len(df_ventas)} registros")
print(f"   Columnas: {list(df_ventas.columns)}")

# Leer clientes (JSON)
print("\n👥 Leyendo clientes...")
with open(path_clientes, 'r', encoding='utf-8') as f:
    clientes_data = json.load(f)
df_clientes = pd.DataFrame(clientes_data)
print(f"   ✅ Clientes cargados: {len(df_clientes)} registros")
print(f"   Columnas: {list(df_clientes.columns)}")

# ===============================================
# 3. TRANSFORMACIÓN - Limpieza y enriquecimiento
# ===============================================
print("\n🔧 PASO 2: TRANSFORMACIÓN DE DATOS")
print("-" * 70)

# 3.1 Agregar metadatos de procesamiento
print("\n📝 Agregando metadatos de procesamiento...")
fecha_procesamiento = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

df_productos['fecha_carga'] = fecha_procesamiento
df_productos['fuente_dato'] = 'productos'

df_ventas['fecha_carga'] = fecha_procesamiento
df_ventas['fuente_dato'] = 'ventas'

df_clientes['fecha_carga'] = fecha_procesamiento
df_clientes['fuente_dato'] = 'clientes'

print(f"   ✅ Metadatos agregados a todos los datasets")

# 3.2 Validación de datos
print("\n🔍 Validando calidad de datos...")

# Verificar valores nulos
print(f"   Productos - Valores nulos: {df_productos.isnull().sum().sum()}")
print(f"   Ventas - Valores nulos: {df_ventas.isnull().sum().sum()}")
print(f"   Clientes - Valores nulos: {df_clientes.isnull().sum().sum()}")

# Verificar duplicados
print(f"   Productos - Duplicados: {df_productos.duplicated(subset=['producto_id']).sum()}")
print(f"   Ventas - Duplicados: {df_ventas.duplicated(subset=['venta_id']).sum()}")
print(f"   Clientes - Duplicados: {df_clientes.duplicated(subset=['cliente_id']).sum()}")

# 3.3 Crear dataset consolidado (JOIN)
print("\n🔗 Creando dataset consolidado (JOIN)...")

# JOIN: ventas + productos
print("   Uniendo ventas con productos...")
df_ventas_productos = df_ventas.merge(
    df_productos[['producto_id', 'nombre', 'categoria', 'precio']],
    on='producto_id',
    how='left',
    suffixes=('_venta', '_producto')
)
print(f"   ✅ Ventas + Productos: {len(df_ventas_productos)} registros")

# JOIN: ventas_productos + clientes
print("   Uniendo con clientes...")
df_consolidado = df_ventas_productos.merge(
    df_clientes[['cliente_id', 'nombre', 'apellido', 'ciudad', 'es_premium']],
    on='cliente_id',
    how='left',
    suffixes=('', '_cliente')
)
print(f"   ✅ Dataset consolidado: {len(df_consolidado)} registros")

# Renombrar columnas para claridad
print("\n🏷️ Renombrando columnas...")
print(f"   Columnas actuales: {list(df_consolidado.columns)}")

# Identificar qué columnas necesitamos renombrar
# Después del merge tenemos: 'nombre' (del producto) y 'nombre_cliente' (del cliente)
df_consolidado.rename(columns={
    'nombre': 'nombre_producto',
    'precio': 'precio_catalogo'
}, inplace=True)

print(f"   ✅ Columnas después de renombrar: {list(df_consolidado.columns)}")

# 3.4 Agregar métricas calculadas
print("\n📊 Calculando métricas adicionales...")

# Diferencia entre precio de venta y catálogo (si la columna existe)
if 'precio_catalogo' in df_consolidado.columns:
    df_consolidado['diferencia_precio'] = df_consolidado['precio_unitario'] - df_consolidado['precio_catalogo']
else:
    # Usar la columna 'precio' que viene del merge
    df_consolidado['diferencia_precio'] = df_consolidado['precio_unitario'] - df_consolidado['precio']

# Descuento en valor absoluto
df_consolidado['descuento_valor'] = df_consolidado['subtotal'] * df_consolidado['descuento_aplicado']

# Categorizar ventas por monto
def categorizar_venta(total):
    if total < 50:
        return 'Baja'
    elif total < 150:
        return 'Media'
    else:
        return 'Alta'

df_consolidado['categoria_venta'] = df_consolidado['total'].apply(categorizar_venta)

print(f"   ✅ Métricas calculadas")

# ===============================================
# 4. CARGA - Guardar en SQLite
# ===============================================
print("\n💾 PASO 3: CARGA DE DATOS")
print("-" * 70)

# Crear conexión a SQLite
print(f"\n🗄️ Creando base de datos SQLite: {db_path}")
conn = sqlite3.connect(db_path)

# Cargar tablas individuales
print("\n📤 Cargando tablas individuales...")
df_productos.to_sql('productos', conn, if_exists='replace', index=False)
print(f"   ✅ Tabla 'productos': {len(df_productos)} registros")

df_ventas.to_sql('ventas', conn, if_exists='replace', index=False)
print(f"   ✅ Tabla 'ventas': {len(df_ventas)} registros")

df_clientes.to_sql('clientes', conn, if_exists='replace', index=False)
print(f"   ✅ Tabla 'clientes': {len(df_clientes)} registros")

# Cargar tabla consolidada
print("\n📤 Cargando tabla consolidada...")
df_consolidado.to_sql('ventas_consolidadas', conn, if_exists='replace', index=False)
print(f"   ✅ Tabla 'ventas_consolidadas': {len(df_consolidado)} registros")

# ===============================================
# 5. VALIDACIÓN - Verificar carga
# ===============================================
print("\n✅ PASO 4: VALIDACIÓN DE RESULTADOS")
print("-" * 70)

# Consultar y mostrar resumen
print("\n📊 Resumen de datos cargados:")

query = """
SELECT 
    COUNT(*) as total_ventas,
    SUM(total) as ingresos_totales,
    AVG(total) as ticket_promedio,
    COUNT(DISTINCT cliente_id) as clientes_unicos,
    COUNT(DISTINCT producto_id) as productos_vendidos
FROM ventas_consolidadas
"""

resultado = pd.read_sql_query(query, conn)
print("\n" + resultado.to_string(index=False))

# Ventas por categoría
print("\n📈 Ventas por categoría de producto:")
query_categoria = """
SELECT 
    categoria,
    COUNT(*) as num_ventas,
    ROUND(SUM(total), 2) as ingresos,
    ROUND(AVG(total), 2) as ticket_promedio
FROM ventas_consolidadas
GROUP BY categoria
ORDER BY ingresos DESC
"""
resultado_cat = pd.read_sql_query(query_categoria, conn)
print("\n" + resultado_cat.to_string(index=False))

# ===============================================
# 6. EXPORTAR DATASET FINAL
# ===============================================
print("\n💾 PASO 5: EXPORTAR DATASET FINAL")
print("-" * 70)

# Guardar CSV consolidado
csv_output = os.path.join(output_path, "ventas_consolidadas_final.csv")
df_consolidado.to_csv(csv_output, index=False, encoding='utf-8')
print(f"✅ Dataset final exportado: {csv_output}")
print(f"   Registros: {len(df_consolidado)}")
print(f"   Columnas: {len(df_consolidado.columns)}")

# Cerrar conexión
conn.close()

# ===============================================
# 7. RESUMEN FINAL
# ===============================================
print("\n" + "="*70)
print("🎉 PROCESO ETL COMPLETADO EXITOSAMENTE")
print("="*70)
print(f"\n📊 RESUMEN DEL PROCESO:")
print(f"   • Archivos de entrada: 3 (productos, ventas, clientes)")
print(f"   • Registros procesados: {len(df_productos) + len(df_ventas) + len(df_clientes)}")
print(f"   • Tablas creadas en BD: 4")
print(f"   • Dataset consolidado: {len(df_consolidado)} registros × {len(df_consolidado.columns)} columnas")
print(f"   • Base de datos: {db_path}")
print(f"   • CSV final: {csv_output}")
print(f"\n✅ Fecha de procesamiento: {fecha_procesamiento}")
print("="*70)

# Mostrar primeras filas del dataset consolidado
print("\n📋 MUESTRA DEL DATASET CONSOLIDADO (primeras 5 filas):")
print(df_consolidado.head().to_string())
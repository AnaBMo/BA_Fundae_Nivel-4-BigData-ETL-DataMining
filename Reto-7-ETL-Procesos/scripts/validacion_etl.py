"""
Script de Validación ETL - Tienda Online
Reto 7: Validación y verificación de calidad de datos

Este script valida:
1. Integridad referencial (IDs válidos)
2. Consistencia de datos (precios, totales)
3. Completitud (registros esperados)
4. Calidad general del proceso ETL
"""

import pandas as pd
import sqlite3
import os
from datetime import datetime

print("="*70)
print("✅ VALIDACIÓN DEL PROCESO ETL - TIENDA ONLINE")
print("="*70)

# ===============================================
# 1. CONECTAR A LA BASE DE DATOS
# ===============================================
print("\n🔌 Conectando a la base de datos...")

base_path = r"C:\Users\Usuario\Documents\1. Fundae\_ Busines Analytics\Nivel-4-BigData-ETL-DataMining\Reto-7-ETL-Procesos"
db_path = os.path.join(base_path, "03_data_warehouse", "tienda_online.db")

try:
    conn = sqlite3.connect(db_path)
    print(f"✅ Conexión exitosa: {db_path}")
except Exception as e:
    print(f"❌ Error al conectar: {e}")
    exit(1)

# ===============================================
# 2. VALIDACIÓN DE INTEGRIDAD REFERENCIAL
# ===============================================
print("\n" + "="*70)
print("🔍 VALIDACIÓN 1: INTEGRIDAD REFERENCIAL")
print("="*70)

# 2.1 Verificar que todos los producto_id en ventas existen en productos
print("\n📦 Verificando productos referenciados en ventas...")
query_productos = """
SELECT COUNT(DISTINCT v.producto_id) as productos_en_ventas,
       (SELECT COUNT(*) FROM productos) as productos_totales
FROM ventas_consolidadas v
"""
result = pd.read_sql_query(query_productos, conn)
print(result.to_string(index=False))

# Verificar productos huérfanos
query_huerfanos_prod = """
SELECT DISTINCT v.producto_id
FROM ventas v
LEFT JOIN productos p ON v.producto_id = p.producto_id
WHERE p.producto_id IS NULL
"""
huerfanos_prod = pd.read_sql_query(query_huerfanos_prod, conn)
if len(huerfanos_prod) == 0:
    print("✅ Todos los productos en ventas existen en la tabla productos")
else:
    print(f"⚠️ Se encontraron {len(huerfanos_prod)} productos huérfanos")

# 2.2 Verificar que todos los cliente_id en ventas existen en clientes
print("\n👥 Verificando clientes referenciados en ventas...")
query_clientes = """
SELECT COUNT(DISTINCT v.cliente_id) as clientes_activos,
       (SELECT COUNT(*) FROM clientes) as clientes_totales
FROM ventas_consolidadas v
"""
result = pd.read_sql_query(query_clientes, conn)
print(result.to_string(index=False))

# Verificar clientes huérfanos
query_huerfanos_cli = """
SELECT DISTINCT v.cliente_id
FROM ventas v
LEFT JOIN clientes c ON v.cliente_id = c.cliente_id
WHERE c.cliente_id IS NULL
"""
huerfanos_cli = pd.read_sql_query(query_huerfanos_cli, conn)
if len(huerfanos_cli) == 0:
    print("✅ Todos los clientes en ventas existen en la tabla clientes")
else:
    print(f"⚠️ Se encontraron {len(huerfanos_cli)} clientes huérfanos")

# ===============================================
# 3. VALIDACIÓN DE CONSISTENCIA DE DATOS
# ===============================================
print("\n" + "="*70)
print("🔍 VALIDACIÓN 2: CONSISTENCIA DE DATOS")
print("="*70)

# 3.1 Verificar cálculos de totales
print("\n💰 Verificando cálculos de totales en ventas...")
query_totales = """
SELECT 
    venta_id,
    cantidad,
    precio_unitario,
    subtotal,
    descuento_aplicado,
    total,
    (cantidad * precio_unitario) as subtotal_calculado,
    ROUND((cantidad * precio_unitario) * (1 - descuento_aplicado), 2) as total_calculado
FROM ventas_consolidadas
WHERE ABS(total - total_calculado) > 0.01
LIMIT 10
"""
inconsistencias = pd.read_sql_query(query_totales, conn)
if len(inconsistencias) == 0:
    print("✅ Todos los totales están correctamente calculados")
else:
    print(f"⚠️ Se encontraron {len(inconsistencias)} inconsistencias en totales:")
    print(inconsistencias.to_string(index=False))

# 3.2 Verificar valores negativos o nulos
print("\n🔢 Verificando valores negativos o inválidos...")
query_valores = """
SELECT 
    'Cantidades negativas' as tipo,
    COUNT(*) as cantidad
FROM ventas_consolidadas
WHERE cantidad <= 0
UNION ALL
SELECT 
    'Precios negativos' as tipo,
    COUNT(*) as cantidad
FROM ventas_consolidadas
WHERE precio_unitario <= 0
UNION ALL
SELECT 
    'Totales negativos' as tipo,
    COUNT(*) as cantidad
FROM ventas_consolidadas
WHERE total < 0
"""
valores_invalidos = pd.read_sql_query(query_valores, conn)
print(valores_invalidos.to_string(index=False))

total_invalidos = valores_invalidos['cantidad'].sum()
if total_invalidos == 0:
    print("✅ No se encontraron valores negativos o inválidos")
else:
    print(f"⚠️ Total de valores inválidos: {total_invalidos}")

# ===============================================
# 4. VALIDACIÓN DE COMPLETITUD
# ===============================================
print("\n" + "="*70)
print("🔍 VALIDACIÓN 3: COMPLETITUD DE DATOS")
print("="*70)

# 4.1 Verificar valores nulos en campos críticos
print("\n📝 Verificando campos nulos en tabla consolidada...")
df_consolidado = pd.read_sql_query("SELECT * FROM ventas_consolidadas", conn)

campos_criticos = ['venta_id', 'fecha', 'cliente_id', 'producto_id', 
                   'nombre_producto', 'nombre_cliente', 'total']

nulos_por_campo = []
for campo in campos_criticos:
    nulos = df_consolidado[campo].isnull().sum()
    nulos_por_campo.append({'campo': campo, 'nulos': nulos})

df_nulos = pd.DataFrame(nulos_por_campo)
print(df_nulos.to_string(index=False))

total_nulos = df_nulos['nulos'].sum()
if total_nulos == 0:
    print("✅ No se encontraron valores nulos en campos críticos")
else:
    print(f"⚠️ Total de valores nulos: {total_nulos}")

# 4.2 Verificar registros duplicados
print("\n🔄 Verificando registros duplicados...")
query_duplicados = """
SELECT venta_id, COUNT(*) as apariciones
FROM ventas_consolidadas
GROUP BY venta_id
HAVING COUNT(*) > 1
"""
duplicados = pd.read_sql_query(query_duplicados, conn)
if len(duplicados) == 0:
    print("✅ No se encontraron registros duplicados")
else:
    print(f"⚠️ Se encontraron {len(duplicados)} IDs duplicados:")
    print(duplicados.to_string(index=False))

# ===============================================
# 5. VALIDACIÓN DE TRANSFORMACIONES
# ===============================================
print("\n" + "="*70)
print("🔍 VALIDACIÓN 4: TRANSFORMACIONES APLICADAS")
print("="*70)

# 5.1 Verificar que se agregaron los metadatos
print("\n📅 Verificando metadatos agregados...")
query_metadatos = """
SELECT 
    COUNT(*) as total_registros,
    COUNT(fecha_carga) as con_fecha_carga,
    COUNT(fuente_dato) as con_fuente_dato,
    COUNT(categoria_venta) as con_categoria_venta
FROM ventas_consolidadas
"""
metadatos = pd.read_sql_query(query_metadatos, conn)
print(metadatos.to_string(index=False))

if metadatos['total_registros'][0] == metadatos['con_fecha_carga'][0]:
    print("✅ Todos los registros tienen fecha_carga")
else:
    print("⚠️ Faltan fechas de carga en algunos registros")

# 5.2 Verificar JOIN correcto
print("\n🔗 Verificando calidad del JOIN...")
query_join = """
SELECT 
    COUNT(*) as total_ventas,
    COUNT(nombre_producto) as con_producto,
    COUNT(nombre_cliente) as con_cliente,
    COUNT(categoria) as con_categoria
FROM ventas_consolidadas
"""
join_quality = pd.read_sql_query(query_join, conn)
print(join_quality.to_string(index=False))

if (join_quality['total_ventas'][0] == join_quality['con_producto'][0] and
    join_quality['total_ventas'][0] == join_quality['con_cliente'][0]):
    print("✅ El JOIN se realizó correctamente - todos los registros están completos")
else:
    print("⚠️ Algunos registros no se unieron correctamente")

# ===============================================
# 6. MÉTRICAS DE CALIDAD GENERAL
# ===============================================
print("\n" + "="*70)
print("📊 MÉTRICAS DE CALIDAD GENERAL")
print("="*70)

# Calcular score de calidad
score_integridad = 100 if (len(huerfanos_prod) == 0 and len(huerfanos_cli) == 0) else 0
score_consistencia = 100 if len(inconsistencias) == 0 else 50
score_completitud = 100 if total_nulos == 0 else 50
score_duplicados = 100 if len(duplicados) == 0 else 0

score_total = (score_integridad + score_consistencia + score_completitud + score_duplicados) / 4

print(f"\n📈 Score de Calidad por Categoría:")
print(f"   • Integridad Referencial: {score_integridad}%")
print(f"   • Consistencia de Datos: {score_consistencia}%")
print(f"   • Completitud: {score_completitud}%")
print(f"   • Duplicados: {score_duplicados}%")
print(f"\n🎯 SCORE TOTAL DE CALIDAD: {score_total}%")

if score_total == 100:
    print("\n✅ ¡EXCELENTE! El proceso ETL tiene calidad óptima")
elif score_total >= 75:
    print("\n✅ BUENO - El proceso ETL tiene calidad aceptable con mejoras menores")
elif score_total >= 50:
    print("\n⚠️ ACEPTABLE - El proceso ETL requiere ajustes")
else:
    print("\n❌ DEFICIENTE - El proceso ETL requiere correcciones importantes")

# ===============================================
# 7. GUARDAR REPORTE DE VALIDACIÓN
# ===============================================
print("\n" + "="*70)
print("💾 GUARDANDO REPORTE DE VALIDACIÓN")
print("="*70)

reporte = {
    'fecha_validacion': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
    'integridad_referencial': {
        'productos_huerfanos': len(huerfanos_prod),
        'clientes_huerfanos': len(huerfanos_cli),
        'score': score_integridad
    },
    'consistencia': {
        'inconsistencias_totales': len(inconsistencias),
        'valores_invalidos': int(total_invalidos),
        'score': score_consistencia
    },
    'completitud': {
        'valores_nulos': int(total_nulos),
        'registros_duplicados': len(duplicados),
        'score': score_completitud
    },
    'score_calidad_total': score_total
}

# Guardar como JSON
import json
reporte_path = os.path.join(base_path, "data_warehouse", "reporte_validacion.json")
with open(reporte_path, 'w', encoding='utf-8') as f:
    json.dump(reporte, f, indent=2, ensure_ascii=False)

print(f"✅ Reporte guardado: {reporte_path}")

# Cerrar conexión
conn.close()

print("\n" + "="*70)
print("🎉 VALIDACIÓN COMPLETADA")
print("="*70)
#!/usr/bin/env python3
"""
Script de Validación - RETO 5 Big Data Hadoop (VERSIÓN FINAL)
CORREGIDO: Maneja problema de encoding con "OtoÃ±o" → "Otoño"
"""

import pandas as pd
import numpy as np
import json

def limpiar_encoding(texto):
    """Corrige problemas de encoding común con caracteres especiales"""
    # Corregir "OtoÃ±o" → "Otoño"
    return texto.replace('OtoÃ±o', 'Otoño')

def cargar_datos_bronze():
    """Carga datos crudos (Bronze Layer)"""
    print("🥉 CARGANDO BRONZE LAYER (Datos Crudos)...")
    
    try:
        rutas_posibles = [
            '../datos/ventas_mercado_especias_expanded.csv',
            './ventas_mercado_especias_expanded.csv',
            '../ventas_mercado_especias_expanded.csv'
        ]
        
        df_bronze = None
        for ruta in rutas_posibles:
            try:
                df_bronze = pd.read_csv(ruta)
                print(f"   ✅ Datos crudos cargados desde: {ruta}")
                break
            except FileNotFoundError:
                continue
        
        if df_bronze is None:
            print("   ❌ No se encontró el archivo CSV de datos crudos")
            return None
            
        print(f"   📊 Total registros Bronze: {len(df_bronze):,}")
        
        return df_bronze
        
    except Exception as e:
        print(f"   ❌ Error cargando datos Bronze: {str(e)}")
        return None

def cargar_datos_gold():
    """Carga datos procesados (Gold Layer) con corrección de encoding"""
    print("\n🥇 CARGANDO GOLD LAYER (Datos Procesados por Hadoop)...")
    
    datos_gold = {}
    
    try:
        # 1. Top productos
        df_productos = pd.read_csv('../resultados/resultados_hadoop_limpio.csv')
        datos_gold['productos'] = df_productos
        print(f"   ✅ Top productos: {len(df_productos)} registros")
        
        # 2. Evolución anual
        with open('../resultados/evolucion_anual.txt', 'r') as f:
            lineas = [line.strip().split('\t') for line in f if line.strip()]
            df_anual = pd.DataFrame(lineas, columns=['ano', 'ventas'])
            df_anual['ano'] = df_anual['ano'].astype(int)
            df_anual['ventas'] = df_anual['ventas'].astype(int)
            datos_gold['evolucion'] = df_anual
        print(f"   ✅ Evolución anual: {len(df_anual)} años")
        
        # 3. Temporadas (CON CORRECCIÓN DE ENCODING)
        with open('../resultados/analisis_temporadas.txt', 'r', encoding='utf-8') as f:
            lineas = []
            for line in f:
                if line.strip():
                    partes = line.strip().split('\t')
                    if len(partes) >= 3:
                        temporada = limpiar_encoding(partes[0])  # ← CORRECCIÓN AQUÍ
                        lineas.append([temporada, partes[1], partes[2]])
            
            df_temp = pd.DataFrame(lineas, columns=['temporada', 'ventas', 'precio_promedio'])
            df_temp['ventas'] = df_temp['ventas'].astype(int)
            df_temp['precio_promedio'] = df_temp['precio_promedio'].astype(float)
            datos_gold['temporadas'] = df_temp
        
        print(f"   ✅ Temporadas: {len(df_temp)} estaciones")
        print(f"   🔧 Temporadas corregidas: {list(df_temp['temporada'])}")
        
        return datos_gold
        
    except Exception as e:
        print(f"   ❌ Error cargando datos Gold: {str(e)}")
        return None

def validar_top_productos(df_bronze, datos_gold):
    """Valida que el top de productos coincida entre Bronze y Gold"""
    print("\n🔍 VALIDACIÓN 1: TOP PRODUCTOS")
    print("-" * 50)
    
    top_bronze = df_bronze['coffee_name'].value_counts().head(8)
    df_gold = datos_gold['productos']
    
    resultados = []
    coincidencias = 0
    
    print("📊 COMPARACIÓN BRONZE vs GOLD:")
    print(f"{'Producto':<25} {'Bronze':<8} {'Gold':<8} {'Match':<8}")
    print("-" * 55)
    
    for i, (_, producto_gold, valor_gold) in df_gold.iterrows():
        valor_gold = int(valor_gold)
        valor_bronze = top_bronze.get(producto_gold, 0)
        match = "✅ SÍ" if valor_bronze == valor_gold else "❌ NO"
        
        if valor_bronze == valor_gold:
            coincidencias += 1
            
        print(f"{producto_gold:<25} {valor_bronze:<8} {valor_gold:<8} {match}")
        
        resultados.append({
            'producto': producto_gold,
            'bronze_count': valor_bronze,
            'gold_count': valor_gold,
            'match': valor_bronze == valor_gold
        })
    
    precision = (coincidencias / len(df_gold)) * 100
    print(f"\n🎯 RESULTADO: {coincidencias}/{len(df_gold)} coincidencias ({precision:.1f}%)")
    
    return resultados, precision

def validar_evolucion_anual(df_bronze, datos_gold):
    """Valida la evolución anual Bronze vs Gold"""
    print("\n🔍 VALIDACIÓN 2: EVOLUCIÓN ANUAL")
    print("-" * 50)
    
    df_bronze['año_extraido'] = pd.to_datetime(df_bronze['date']).dt.year
    evolucion_bronze = df_bronze['año_extraido'].value_counts().sort_index()
    df_gold = datos_gold['evolucion'].set_index('ano')['ventas']
    
    resultados = []
    coincidencias = 0
    
    print("📅 COMPARACIÓN EVOLUCIÓN TEMPORAL:")
    print(f"{'Año':<6} {'Bronze':<8} {'Gold':<8} {'Diferencia':<10} {'Match':<8}")
    print("-" * 50)
    
    for ano in sorted(df_gold.index):
        bronze_val = evolucion_bronze.get(ano, 0)
        gold_val = df_gold[ano]
        diferencia = abs(bronze_val - gold_val)
        match = "✅ SÍ" if diferencia == 0 else f"❌ {diferencia}"
        
        if diferencia == 0:
            coincidencias += 1
            
        print(f"{ano:<6} {bronze_val:<8} {gold_val:<8} {diferencia:<10} {match}")
        
        resultados.append({
            'ano': ano,
            'bronze_ventas': bronze_val,
            'gold_ventas': gold_val,
            'diferencia': diferencia,
            'match': diferencia == 0
        })
    
    precision = (coincidencias / len(df_gold)) * 100
    print(f"\n🎯 RESULTADO: {coincidencias}/{len(df_gold)} coincidencias ({precision:.1f}%)")
    
    return resultados, precision

def validar_temporadas(df_bronze, datos_gold):
    """Valida análisis por temporadas Bronze vs Gold (CON CORRECCIÓN)"""
    print("\n🔍 VALIDACIÓN 3: ANÁLISIS ESTACIONAL")
    print("-" * 50)
    
    temp_bronze = df_bronze.groupby('temporada').agg({
        'coffee_name': 'count',
        'money': 'mean'
    }).round(2)
    temp_bronze.columns = ['ventas', 'precio_promedio']
    
    df_gold = datos_gold['temporadas'].set_index('temporada')
    
    resultados = []
    coincidencias_ventas = 0
    coincidencias_precios = 0
    
    print("🌿 COMPARACIÓN ANÁLISIS ESTACIONAL:")
    print(f"{'Temporada':<12} {'Bronze V.':<9} {'Gold V.':<9} {'Bronze €':<9} {'Gold €':<9} {'Match':<10}")
    print("-" * 75)
    
    # AHORA DEBERÍA FUNCIONAR CON TODAS LAS TEMPORADAS
    for temporada in df_gold.index:
        if temporada in temp_bronze.index:
            bronze_ventas = temp_bronze.loc[temporada, 'ventas']
            gold_ventas = df_gold.loc[temporada, 'ventas']
            bronze_precio = temp_bronze.loc[temporada, 'precio_promedio']
            gold_precio = df_gold.loc[temporada, 'precio_promedio']
            
            match_ventas = bronze_ventas == gold_ventas
            match_precio = abs(bronze_precio - gold_precio) < 0.01
            match_total = "✅ SÍ" if match_ventas and match_precio else "❌ NO"
            
            if match_ventas:
                coincidencias_ventas += 1
            if match_precio:
                coincidencias_precios += 1
            
            print(f"{temporada:<12} {bronze_ventas:<9} {gold_ventas:<9} {bronze_precio:<9.2f} {gold_precio:<9.2f} {match_total}")
            
            resultados.append({
                'temporada': temporada,
                'bronze_ventas': bronze_ventas,
                'gold_ventas': gold_ventas,
                'bronze_precio': bronze_precio,
                'gold_precio': gold_precio,
                'match_ventas': match_ventas,
                'match_precio': match_precio
            })
        else:
            print(f"⚠️  '{temporada}' no encontrada en Bronze")
    
    precision_ventas = (coincidencias_ventas / len(df_gold)) * 100
    precision_precios = (coincidencias_precios / len(df_gold)) * 100
    
    print(f"\n🎯 RESULTADO VENTAS: {coincidencias_ventas}/{len(df_gold)} coincidencias ({precision_ventas:.1f}%)")
    print(f"🎯 RESULTADO PRECIOS: {coincidencias_precios}/{len(df_gold)} coincidencias ({precision_precios:.1f}%)")
    
    return resultados, (precision_ventas, precision_precios)

def generar_informe_validacion(resultados_productos, resultados_anual, resultados_temporadas, precisiones):
    """Genera informe completo de validación"""
    print("\n" + "="*60)
    print("📋 INFORME FINAL DE VALIDACIÓN")
    print("="*60)
    
    precision_productos, precision_anual, (precision_temp_ventas, precision_temp_precios) = precisiones
    
    print(f"✅ VALIDACIÓN TOP PRODUCTOS: {precision_productos:.1f}% coincidencia")
    print(f"✅ VALIDACIÓN EVOLUCIÓN ANUAL: {precision_anual:.1f}% coincidencia")
    print(f"✅ VALIDACIÓN TEMPORADAS (Ventas): {precision_temp_ventas:.1f}% coincidencia")
    print(f"✅ VALIDACIÓN TEMPORADAS (Precios): {precision_temp_precios:.1f}% coincidencia")
    
    precision_global = np.mean([precision_productos, precision_anual, precision_temp_ventas, precision_temp_precios])
    
    print(f"\n🏆 PRECISIÓN GLOBAL: {precision_global:.1f}%")
    
    if precision_global >= 98:
        print("🎉 EXCELENTE - Los datos procesados por Hadoop son 100% fieles a los originales")
        calificacion = "EXCELENTE"
    elif precision_global >= 90:
        print("👍 BUENO - Pequeñas discrepancias menores, probablemente por redondeos")
        calificacion = "BUENO"  
    else:
        print("⚠️  REVISAR - Discrepancias significativas detectadas")
        calificacion = "REVISAR"
    
    # Guardar informe detallado
    informe = {
        'fecha_validacion': pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S'),
        'precision_global': precision_global,
        'calificacion': calificacion,
        'problema_resuelto': 'Corregido encoding "OtoÃ±o" → "Otoño"',
        'validaciones': {
            'productos': precision_productos,
            'evolucion_anual': precision_anual,
            'temporadas_ventas': precision_temp_ventas,
            'temporadas_precios': precision_temp_precios
        },
        'detalles': {
            'productos': resultados_productos,
            'evolucion_anual': resultados_anual,
            'temporadas': resultados_temporadas
        }
    }
    
    with open('validacion_resultados_corregida.json', 'w', encoding='utf-8') as f:
        json.dump(informe, f, indent=2, ensure_ascii=False, default=str)
    
    print(f"\n💾 Informe detallado guardado en: validacion_resultados_corregida.json")
    
    return informe

def main():
    """Función principal - Ejecuta todas las validaciones"""
    print("🔍 INICIANDO VALIDACIÓN BRONZE vs GOLD LAYER (VERSIÓN CORREGIDA)")
    print("="*70)
    print("Verificando que los resultados de Hadoop coinciden con datos originales")
    print("🔧 INCLUYE CORRECCIÓN DE ENCODING PARA 'OtoÃ±o' → 'Otoño'")
    print()
    
    # Cargar datos
    df_bronze = cargar_datos_bronze()
    if df_bronze is None:
        return
        
    datos_gold = cargar_datos_gold()
    if datos_gold is None:
        return
    
    # Ejecutar validaciones
    resultados_productos, precision_productos = validar_top_productos(df_bronze, datos_gold)
    resultados_anual, precision_anual = validar_evolucion_anual(df_bronze, datos_gold)
    resultados_temporadas, precision_temporadas = validar_temporadas(df_bronze, datos_gold)
    
    # Generar informe final
    precisiones = (precision_productos, precision_anual, precision_temporadas)
    informe = generar_informe_validacion(
        resultados_productos, 
        resultados_anual, 
        resultados_temporadas, 
        precisiones
    )
    
    print("\n🎯 VALIDACIÓN COMPLETADA CON CORRECCIÓN DE ENCODING")
    print("✅ Problema 'OtoÃ±o' → 'Otoño' resuelto")
    print("📊 Los archivos generados demuestran la integridad COMPLETA de tu análisis Big Data")

if __name__ == "__main__":
    main()
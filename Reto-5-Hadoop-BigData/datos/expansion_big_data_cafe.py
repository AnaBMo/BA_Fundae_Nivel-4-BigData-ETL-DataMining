#!/usr/bin/env python3
"""
Script de Expansión Big Data - Mercado de las Especias
Reto 5: Análisis con Hadoop

Este script toma el café_limpio.csv original y lo expande para simular:
1. Datos históricos (2019-2025)
2. Múltiples fuentes de datos
3. Volumen apropiado para Big Data (~100K+ registros)
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random
import json
from faker import Faker

# Configurar Faker para datos realistas
fake = Faker(['es_ES'])
np.random.seed(42)
random.seed(42)

def cargar_datos_originales(archivo):
    """Carga el dataset original de café"""
    print(f"📊 Cargando dataset original: {archivo}")
    df = pd.read_csv(archivo)
    print(f"✅ Registros originales: {len(df)}")
    return df

def generar_datos_historicos(df_original, años_historicos=5):
    """Genera datos históricos basados en patrones del dataset original"""
    print(f"🔄 Generando {años_historicos} años de datos históricos...")
    
    datos_expandidos = []
    
    # Analizar patrones del dataset original
    productos = df_original['coffee_name'].value_counts()
    precios_por_producto = df_original.groupby('coffee_name')['money'].mean().to_dict()
    
    # Generar para cada año histórico
    for año in range(2024 - años_historicos, 2025):
        # Ajustar volumen por año (crecimiento del negocio)
        factor_crecimiento = 0.85 + (2025 - año) * 0.03
        transacciones_año = int(len(df_original) * factor_crecimiento * random.uniform(0.8, 1.2))
        
        print(f"  Año {año}: {transacciones_año:,} transacciones")
        
        for i in range(transacciones_año):
            # Fecha aleatoria del año
            inicio_año = datetime(año, 1, 1)
            fin_año = datetime(año, 12, 31)
            fecha_random = fake.date_time_between(start_date=inicio_año, end_date=fin_año)
            
            # Producto basado en distribución original
            producto = np.random.choice(productos.index, p=productos.values/productos.sum())
            
            # Precio con variación realista
            precio_base = precios_por_producto[producto]
            variacion = np.random.normal(1.0, 0.1)  # ±10% variación
            precio = round(precio_base * variacion * (0.95 + (2025-año)*0.01), 1)  # Inflación
            
            # Tipo de pago
            cash_type = 'card' if random.random() > 0.15 else 'cash'
            
            # Generar registro
            registro = {
                'date': fecha_random.strftime('%Y-%m-%d'),
                'datetime': fecha_random.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3],
                'cash_type': cash_type,
                'card': f"ANON-{año}-{random.randint(1000, 9999)}-{i:04d}" if cash_type == 'card' else 'PAGO-EFECTIVO',
                'money': precio,
                'coffee_name': producto,
                'es_outlier': random.random() < 0.02,  # 2% outliers
                'mes': fecha_random.month,
                'año': fecha_random.year,
                'dia_semana': fecha_random.weekday(),
                'es_fin_semana': fecha_random.weekday() >= 5,
                'temporada': obtener_temporada(fecha_random.month)
            }
            
            datos_expandidos.append(registro)
    
    return pd.DataFrame(datos_expandidos)

def generar_datos_redes_sociales(df_ventas):
    """Genera dataset de menciones en redes sociales"""
    print("📱 Generando datos de redes sociales...")
    
    # Sentimientos y palabras clave
    sentimientos = ['positivo', 'negativo', 'neutral']
    palabras_positivas = ['excelente', 'delicioso', 'perfecto', 'increíble', 'recomiendo']
    palabras_negativas = ['malo', 'terrible', 'frío', 'caro', 'lento']
    palabras_neutrales = ['café', 'compré', 'probé', 'servicio', 'local']
    
    redes_sociales = []
    
    # Por cada día de ventas, generar menciones en redes sociales
    fechas_unicas = df_ventas['date'].unique()
    
    for fecha in fechas_unicas:
        ventas_dia = df_ventas[df_ventas['date'] == fecha]
        productos_dia = ventas_dia['coffee_name'].value_counts()
        
        # Número de menciones proporcional a las ventas
        num_menciones = int(len(ventas_dia) * random.uniform(0.1, 0.3))
        
        for i in range(num_menciones):
            # Producto mencionado (basado en ventas del día)
            if len(productos_dia) > 0:
                producto = np.random.choice(productos_dia.index, 
                                          p=productos_dia.values/productos_dia.sum())
            else:
                producto = random.choice(df_ventas['coffee_name'].unique())
            
            # Sentimiento
            sentimiento = np.random.choice(sentimientos, p=[0.6, 0.2, 0.2])  # Más positivo
            
            # Generar texto de la mención
            if sentimiento == 'positivo':
                texto = f"{producto} {random.choice(palabras_positivas)} en el Mercado de las Especias"
            elif sentimiento == 'negativo':
                texto = f"{producto} {random.choice(palabras_negativas)} hoy"
            else:
                texto = f"Probé {producto} en {random.choice(palabras_neutrales)}"
            
            # Red social
            plataforma = np.random.choice(['Twitter', 'Instagram', 'Facebook'], p=[0.5, 0.3, 0.2])
            
            registro_social = {
                'fecha': fecha,
                'fecha_hora': fake.date_time_between(
                    start_date=datetime.strptime(fecha, '%Y-%m-%d'),
                    end_date=datetime.strptime(fecha, '%Y-%m-%d') + timedelta(days=1)
                ).strftime('%Y-%m-%d %H:%M:%S'),
                'plataforma': plataforma,
                'producto': producto,
                'sentimiento': sentimiento,
                'texto': texto,
                'usuario': fake.user_name(),
                'likes': random.randint(0, 100) if sentimiento == 'positivo' else random.randint(0, 20),
                'compartidos': random.randint(0, 50),
                'comentarios': random.randint(0, 30)
            }
            
            redes_sociales.append(registro_social)
    
    print(f"✅ Generadas {len(redes_sociales):,} menciones en redes sociales")
    return pd.DataFrame(redes_sociales)

def generar_datos_sensores(df_ventas):
    """Genera datos de sensores del almacén"""
    print("🌡️ Generando datos de sensores...")
    
    sensores_data = []
    fechas_unicas = df_ventas['date'].unique()
    
    # Tipos de sensores
    sensores = ['temperatura', 'humedad', 'stock_level', 'foot_traffic']
    
    for fecha in fechas_unicas:
        fecha_dt = datetime.strptime(fecha, '%Y-%m-%d')
        
        # Lecturas cada hora
        for hora in range(24):
            timestamp = fecha_dt + timedelta(hours=hora)
            
            # Simular lecturas de sensores
            for sensor in sensores:
                if sensor == 'temperatura':
                    # Temperatura del almacén (18-22°C)
                    valor = np.random.normal(20, 1.5)
                    unidad = '°C'
                elif sensor == 'humedad':
                    # Humedad relativa (40-60%)
                    valor = np.random.normal(50, 5)
                    unidad = '%'
                elif sensor == 'stock_level':
                    # Nivel de stock (0-100%)
                    valor = max(0, min(100, np.random.normal(75, 15)))
                    unidad = '%'
                else:  # foot_traffic
                    # Tráfico de personas (más durante horas comerciales)
                    if 8 <= hora <= 20:
                        valor = max(0, np.random.normal(15, 5))
                    else:
                        valor = max(0, np.random.normal(2, 1))
                    unidad = 'personas/hora'
                
                registro_sensor = {
                    'timestamp': timestamp.strftime('%Y-%m-%d %H:%M:%S'),
                    'sensor_id': f"SENS_{sensor.upper()}_{random.randint(1000, 9999)}",
                    'sensor_type': sensor,
                    'value': round(valor, 2),
                    'unit': unidad,
                    'location': 'almacen_principal',
                    'status': 'normal' if random.random() > 0.05 else 'warning'
                }
                
                sensores_data.append(registro_sensor)
    
    print(f"✅ Generados {len(sensores_data):,} registros de sensores")
    return pd.DataFrame(sensores_data)

def obtener_temporada(mes):
    """Determina la temporada basada en el mes"""
    if mes in [12, 1, 2]:
        return 'Invierno'
    elif mes in [3, 4, 5]:
        return 'Primavera'
    elif mes in [6, 7, 8]:
        return 'Verano'
    else:
        return 'Otoño'

def guardar_datasets(df_ventas, df_social, df_sensores, directorio_salida="/home/claude"):
    """Guarda todos los datasets generados"""
    print("💾 Guardando datasets...")
    
    # Ventas expandidas
    archivo_ventas = f"{directorio_salida}/ventas_mercado_especias_expanded.csv"
    df_ventas.to_csv(archivo_ventas, index=False)
    print(f"✅ Ventas guardadas: {archivo_ventas} ({len(df_ventas):,} registros)")
    
    # Redes sociales
    archivo_social = f"{directorio_salida}/social_media_menciones.csv"
    df_social.to_csv(archivo_social, index=False)
    print(f"✅ Redes sociales guardadas: {archivo_social} ({len(df_social):,} registros)")
    
    # Sensores
    archivo_sensores = f"{directorio_salida}/sensores_almacen.csv"
    df_sensores.to_csv(archivo_sensores, index=False)
    print(f"✅ Sensores guardados: {archivo_sensores} ({len(df_sensores):,} registros)")
    
    # Resumen para Hadoop
    resumen = {
        "datasets_generados": {
            "ventas": {"archivo": archivo_ventas, "registros": len(df_ventas), "tamaño_mb": round(df_ventas.memory_usage(deep=True).sum() / 1024**2, 2)},
            "social_media": {"archivo": archivo_social, "registros": len(df_social), "tamaño_mb": round(df_social.memory_usage(deep=True).sum() / 1024**2, 2)},
            "sensores": {"archivo": archivo_sensores, "registros": len(df_sensores), "tamaño_mb": round(df_sensores.memory_usage(deep=True).sum() / 1024**2, 2)}
        },
        "total_registros": len(df_ventas) + len(df_social) + len(df_sensores),
        "fecha_generacion": datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    }
    
    with open(f"{directorio_salida}/datasets_summary.json", 'w') as f:
        json.dump(resumen, f, indent=2, ensure_ascii=False)
    
    print(f"\n🎯 RESUMEN TOTAL:")
    print(f"   📊 Total de registros: {resumen['total_registros']:,}")
    print(f"   📁 Archivos generados: 3 datasets + 1 resumen")
    print(f"   💽 Tamaño aproximado: {sum([d['tamaño_mb'] for d in resumen['datasets_generados'].values()]):.1f} MB")
    
    return resumen

def main():
    """Función principal"""
    print("🚀 INICIANDO EXPANSIÓN BIG DATA - MERCADO DE LAS ESPECIAS")
    print("=" * 60)
    
    try:
        # 1. Cargar datos originales
        df_original = cargar_datos_originales('/mnt/user-data/uploads/cafe_limpio.csv')
        
        # 2. Generar datos históricos de ventas
        df_ventas_expanded = generar_datos_historicos(df_original, años_historicos=5)
        
        # Combinar datos originales con históricos
        df_ventas_total = pd.concat([df_ventas_expanded, df_original], ignore_index=True)
        df_ventas_total = df_ventas_total.sort_values('datetime').reset_index(drop=True)
        
        print(f"✅ Total ventas (original + histórico): {len(df_ventas_total):,}")
        
        # 3. Generar datos de redes sociales
        df_social = generar_datos_redes_sociales(df_ventas_total)
        
        # 4. Generar datos de sensores
        df_sensores = generar_datos_sensores(df_ventas_total)
        
        # 5. Guardar todos los datasets
        resumen = guardar_datasets(df_ventas_total, df_social, df_sensores)
        
        print("\n🎉 ¡EXPANSIÓN COMPLETADA EXITOSAMENTE!")
        print("📋 Los datos están listos para Hadoop/HDFS")
        print("=" * 60)
        
    except Exception as e:
        print(f"❌ Error durante la expansión: {str(e)}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()

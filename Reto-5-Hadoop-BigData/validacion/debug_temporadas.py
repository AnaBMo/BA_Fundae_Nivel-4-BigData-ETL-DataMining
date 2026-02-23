#!/usr/bin/env python3
"""
Script de DEBUG - Encontrar por qué falta Otoño en la validación
"""

import pandas as pd

def debug_temporadas():
    """Función de debug para encontrar el problema con Otoño"""
    print("🔍 DEBUG: INVESTIGANDO PROBLEMA CON OTOÑO")
    print("="*60)
    
    # 1. Cargar datos Gold
    print("1️⃣ DATOS GOLD (Hadoop):")
    with open('../resultados/analisis_temporadas.txt', 'r') as f:
        lineas = [line.strip().split('\t') for line in f if line.strip()]
        df_gold = pd.DataFrame(lineas, columns=['temporada', 'ventas', 'precio_promedio'])
        df_gold['ventas'] = df_gold['ventas'].astype(int)
        df_gold['precio_promedio'] = df_gold['precio_promedio'].astype(float)
        df_gold_indexed = df_gold.set_index('temporada')
    
    print(f"   Temporadas Gold: {list(df_gold['temporada'])}")
    print(f"   Índices Gold: {list(df_gold_indexed.index)}")
    for temp in df_gold['temporada']:
        print(f"   '{temp}' -> bytes: {temp.encode('utf-8')}")
    
    # 2. Cargar datos Bronze  
    print("\n2️⃣ DATOS BRONZE (CSV):")
    df_bronze = pd.read_csv('../datos/ventas_mercado_especias_expanded.csv')
    temp_bronze = df_bronze.groupby('temporada').agg({
        'coffee_name': 'count',
        'money': 'mean'
    }).round(2)
    temp_bronze.columns = ['ventas', 'precio_promedio']
    
    print(f"   Temporadas Bronze: {list(temp_bronze.index)}")
    for temp in temp_bronze.index:
        print(f"   '{temp}' -> bytes: {temp.encode('utf-8')}")
    
    # 3. Testing matches individuales
    print("\n3️⃣ TESTING MATCHES:")
    for temp_gold in df_gold_indexed.index:
        match = temp_gold in temp_bronze.index
        print(f"   '{temp_gold}' en Bronze: {match}")
        
        if not match:
            print(f"   🔍 PROBLEMA: '{temp_gold}' no hace match")
            # Buscar similares
            for temp_bronze_item in temp_bronze.index:
                if "oto" in temp_bronze_item.lower() and "oto" in temp_gold.lower():
                    print(f"   💡 Candidato similar: '{temp_bronze_item}'")
    
    # 4. Comparación byte a byte de "Otoño"
    print("\n4️⃣ ANÁLISIS ESPECÍFICO DE 'OTOÑO':")
    otono_gold = None
    otono_bronze = None
    
    for temp in df_gold['temporada']:
        if "oto" in temp.lower():
            otono_gold = temp
            break
            
    for temp in temp_bronze.index:
        if "oto" in temp.lower():
            otono_bronze = temp
            break
    
    if otono_gold and otono_bronze:
        print(f"   Gold Otoño: '{otono_gold}' -> {otono_gold.encode('utf-8')}")
        print(f"   Bronze Otoño: '{otono_bronze}' -> {otono_bronze.encode('utf-8')}")
        print(f"   Son iguales: {otono_gold == otono_bronze}")
        print(f"   Longitud Gold: {len(otono_gold)}, Bronze: {len(otono_bronze)}")
    
    # 5. Simulación del loop problemático
    print("\n5️⃣ SIMULACIÓN DEL LOOP PROBLEMÁTICO:")
    print("   for temporada in df_gold.index:")
    coincidencias = 0
    for temporada in df_gold_indexed.index:
        if temporada in temp_bronze.index:
            print(f"   ✅ PROCESARÍA: {temporada}")
            coincidencias += 1
        else:
            print(f"   ❌ SALTARÍA: {temporada} (no está en Bronze index)")
    
    print(f"\n📊 RESULTADO: {coincidencias}/{len(df_gold_indexed)} temporadas procesadas")
    
    return df_gold_indexed, temp_bronze

if __name__ == "__main__":
    debug_temporadas()

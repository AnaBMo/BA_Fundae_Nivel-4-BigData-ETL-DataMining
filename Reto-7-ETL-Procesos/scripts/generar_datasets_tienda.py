import pandas as pd
import numpy as np
import json
from datetime import datetime, timedelta
import random

# Configurar semilla para reproducibilidad
np.random.seed(42)
random.seed(42)

# ===================================
# 1. DATASET: PRODUCTOS (CSV)
# ===================================
print("📦 Generando dataset de productos...")

categorias = ['Electrónica', 'Ropa', 'Hogar', 'Deportes', 'Libros']
productos_por_categoria = {
    'Electrónica': ['Auriculares Bluetooth', 'Mouse Inalámbrico', 'Teclado Mecánico', 'Webcam HD', 'Cable USB-C', 
                    'Cargador Portátil', 'Memoria USB 32GB', 'Hub USB', 'Adaptador HDMI', 'Lámpara LED'],
    'Ropa': ['Camiseta Básica', 'Jeans Clásicos', 'Sudadera con Capucha', 'Zapatillas Deportivas', 'Chaqueta Impermeable',
             'Gorra Deportiva', 'Calcetines Pack 3', 'Bufanda Lana', 'Guantes Invierno', 'Cinturón Cuero'],
    'Hogar': ['Juego Sábanas', 'Toallas Pack 4', 'Cojín Decorativo', 'Cortinas Blackout', 'Alfombra Sala',
              'Lámpara Mesa', 'Espejo Pared', 'Reloj Pared', 'Marco Fotos', 'Velas Aromáticas Pack 3'],
    'Deportes': ['Botella Agua 1L', 'Yoga Mat', 'Pesas 2kg Par', 'Cuerda Saltar', 'Banda Elástica',
                 'Guantes Gym', 'Mochila Deportiva', 'Toalla Microfibra', 'Rodilleras', 'Cronómetro Digital'],
    'Libros': ['Novela Misterio', 'Libro Cocina', 'Guía Viajes Europa', 'Biografía Histórica', 'Manual Python',
               'Libro Autoayuda', 'Cómic Aventuras', 'Enciclopedia Natural', 'Poesía Clásica', 'Thriller Político']
}

productos_data = []
producto_id = 1

for categoria, productos in productos_por_categoria.items():
    for producto in productos:
        # Precio según categoría
        if categoria == 'Electrónica':
            precio = round(random.uniform(15, 80), 2)
        elif categoria == 'Ropa':
            precio = round(random.uniform(12, 60), 2)
        elif categoria == 'Hogar':
            precio = round(random.uniform(8, 50), 2)
        elif categoria == 'Deportes':
            precio = round(random.uniform(10, 45), 2)
        else:  # Libros
            precio = round(random.uniform(10, 35), 2)
        
        productos_data.append({
            'producto_id': f'PROD{producto_id:03d}',
            'nombre': producto,
            'categoria': categoria,
            'precio': precio,
            'stock_inicial': random.randint(20, 200)
        })
        producto_id += 1

df_productos = pd.DataFrame(productos_data)
print(f"✅ Productos creados: {len(df_productos)} productos")

# ===================================
# 2. DATASET: CLIENTES (JSON)
# ===================================
print("\n👥 Generando dataset de clientes...")

nombres = ['Ana', 'Carlos', 'María', 'Juan', 'Laura', 'Pedro', 'Sofía', 'Diego', 'Carmen', 'Luis',
           'Elena', 'Miguel', 'Isabel', 'Javier', 'Raquel', 'Antonio', 'Beatriz', 'Fernando', 'Patricia', 'Roberto']
apellidos = ['García', 'Martínez', 'López', 'Sánchez', 'González', 'Pérez', 'Rodríguez', 'Fernández', 
             'Díaz', 'Torres', 'Ruiz', 'Ramírez', 'Flores', 'Morales', 'Jiménez']
ciudades = ['Madrid', 'Barcelona', 'Valencia', 'Sevilla', 'Zaragoza', 'Málaga', 'Bilbao', 'Murcia']

clientes_data = []

for i in range(1, 101):
    cliente = {
        'cliente_id': f'CLI{i:03d}',
        'nombre': random.choice(nombres),
        'apellido': random.choice(apellidos),
        'email': f'cliente{i}@email.com',
        'ciudad': random.choice(ciudades),
        'fecha_registro': (datetime(2024, 1, 1) + timedelta(days=random.randint(0, 365))).strftime('%Y-%m-%d'),
        'es_premium': random.choice([True, False])
    }
    clientes_data.append(cliente)

print(f"✅ Clientes creados: {len(clientes_data)} clientes")

# ===================================
# 3. DATASET: VENTAS (CSV)
# ===================================
print("\n💰 Generando dataset de ventas (febrero 2025)...")

ventas_data = []
venta_id = 1

# Generar ventas para febrero 2025 (28 días)
fecha_inicio = datetime(2025, 2, 1)
fecha_fin = datetime(2025, 2, 28)

# Aproximadamente 500 ventas en 28 días (~18 ventas por día)
num_ventas = 500

for i in range(num_ventas):
    # Fecha aleatoria en febrero
    dias_random = random.randint(0, 27)
    hora_random = random.randint(8, 21)  # Horario comercial
    minuto_random = random.randint(0, 59)
    
    fecha_venta = fecha_inicio + timedelta(days=dias_random, hours=hora_random, minutes=minuto_random)
    
    # Cliente aleatorio
    cliente = random.choice(clientes_data)
    
    # Producto aleatorio
    producto = df_productos.sample(1).iloc[0]
    
    # Cantidad (1-5 unidades)
    cantidad = random.randint(1, 5)
    
    # Total
    total = round(producto['precio'] * cantidad, 2)
    
    # Descuento para clientes premium
    descuento = 0.10 if cliente['es_premium'] else 0
    total_con_descuento = round(total * (1 - descuento), 2)
    
    ventas_data.append({
        'venta_id': f'VEN{venta_id:04d}',
        'fecha': fecha_venta.strftime('%Y-%m-%d'),
        'hora': fecha_venta.strftime('%H:%M'),
        'cliente_id': cliente['cliente_id'],
        'producto_id': producto['producto_id'],
        'cantidad': cantidad,
        'precio_unitario': producto['precio'],
        'subtotal': total,
        'descuento_aplicado': descuento,
        'total': total_con_descuento
    })
    venta_id += 1

df_ventas = pd.DataFrame(ventas_data)
print(f"✅ Ventas creadas: {len(df_ventas)} transacciones")

# ===================================
# GUARDAR ARCHIVOS
# ===================================
print("\n💾 Guardando archivos...")

# Guardar CSV
df_productos.to_csv('productos.csv', index=False, encoding='utf-8')
df_ventas.to_csv('ventas.csv', index=False, encoding='utf-8')

# Guardar JSON
with open('clientes.json', 'w', encoding='utf-8') as f:
    json.dump(clientes_data, f, indent=2, ensure_ascii=False)

print("\n" + "="*60)
print("🎉 DATASETS GENERADOS EXITOSAMENTE")
print("="*60)
print(f"\n📊 RESUMEN:")
print(f"   • Productos:  {len(df_productos)} registros (productos.csv)")
print(f"   • Clientes:   {len(clientes_data)} registros (clientes.json)")
print(f"   • Ventas:     {len(df_ventas)} registros (ventas.csv)")
print(f"\n📁 Archivos guardados en la carpeta actual")
print("="*60)

# Mostrar primeras filas de cada dataset
print("\n📦 MUESTRA - PRODUCTOS:")
print(df_productos.head())

print("\n💰 MUESTRA - VENTAS:")
print(df_ventas.head())

print("\n👥 MUESTRA - CLIENTES (primeros 3):")
print(json.dumps(clientes_data[:3], indent=2, ensure_ascii=False))
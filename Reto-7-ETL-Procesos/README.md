# 📊 RETO 7: Proceso ETL con Integración de Datos
## Biblioteca de Alexandata - Tienda Online

---

## 👤 Información del Proyecto

**Programa:** FUNDAE Business Analytics Nivel 4  
**Reto:** 7 - Diseñar y ejecutar proceso ETL  
**Fecha:** Marzo 2026  
**Repositorio:** [GitHub - Reto 7](https://github.com/AnaBMo/BA_Fundae_Nivel-4-BigData-ETL-DataMining)

---

## 🎯 Objetivos del Reto

Diseñar y ejecutar un proceso ETL (Extract, Transform, Load) completo utilizando herramientas modernas de integración de datos para:

1. Extraer datos de múltiples fuentes con diferentes formatos
2. Transformar y unificar los datos aplicando reglas de negocio
3. Cargar los datos procesados en un destino único
4. Validar la calidad e integridad del proceso
5. Documentar cada etapa del proceso ETL

---

## 🏗️ Arquitectura del Proceso ETL

### Enfoque Híbrido Profesional
```
FUENTES DE DATOS
├── productos.csv (50 registros)
├── ventas.csv (500 registros)
└── clientes.json (100 registros)
    ↓
EXTRACT (Apache NiFi 2.8.0)
├── GetFile processors
├── UpdateAttribute (metadatos)
└── PutFile (organización)
    ↓
TRANSFORM (Python 3.12 + Pandas)
├── Agregación de metadatos
├── JOIN de datasets
├── Cálculo de métricas
└── Validación de calidad
    ↓
LOAD (SQLite Database)
├── Tabla: productos (50 registros)
├── Tabla: ventas (500 registros)
├── Tabla: clientes (100 registros)
├── Tabla: ventas_consolidadas (500 registros)
└── CSV final exportado
```

---

## 🛠️ Herramientas Utilizadas

### Software y Tecnologías

| Herramienta | Versión | Propósito |
|------------|---------|-----------|
| Apache NiFi | 2.8.0 | Orquestación ETL y extracción de datos |
| Python | 3.12 | Transformación y procesamiento de datos |
| Pandas | Latest | Manipulación de DataFrames |
| SQLite | 3.x | Almacenamiento del Data Warehouse |
| VSCode | Latest | Desarrollo y edición de código |
| Git/GitHub | Latest | Control de versiones |

### Librerías Python
- pandas
- numpy
- sqlite3 
- json 
- datetime 
- os 

---

## 📥 FASE 1: EXTRACCIÓN (Extract)

### 1.1 Fuentes de Datos

Se generaron 3 datasets sintéticos representando una tienda online:

**Productos (CSV)**
- Archivo: productos.csv
- Registros: 50
- Columnas: producto_id, nombre, categoria, precio, stock_inicial
- Categorías: Electrónica, Ropa, Hogar, Deportes, Libros

**Ventas (CSV)**
- Archivo: ventas.csv
- Registros: 500 (transacciones de febrero 2025)
- Columnas: venta_id, fecha, hora, cliente_id, producto_id, cantidad, precio_unitario, subtotal, descuento_aplicado, total

**Clientes (JSON)**
- Archivo: clientes.json
- Registros: 100
- Campos: cliente_id, nombre, apellido, email, ciudad, fecha_registro, es_premium

### 1.2 Proceso de Extracción con Apache NiFi

**Flujo implementado:**

Cada fuente de datos pasa por 3 procesadores:
1. GetFile - Lee el archivo desde directorio local
2. UpdateAttribute - Agrega metadatos (tipo_fuente, fecha_procesamiento)
3. PutFile - Guarda en estructura Data Warehouse

**Configuración de procesadores:**

GetFile:
- Input Directory: Ruta del proyecto
- File Filter: Nombre específico del archivo
- Keep Source File: true

UpdateAttribute:
- tipo_fuente: Identifica el origen
- fecha_procesamiento: Timestamp del procesamiento

PutFile:
- Directory: /data_warehouse/{tipo_fuente}/
- Conflict Resolution Strategy: replace

**Archivos generados:**
- flujo_nifi_extraccion.json
- flujo_nifi_extraccion.png

---

## 🔧 FASE 2: TRANSFORMACIÓN (Transform)

### 2.1 Script de Transformación

Archivo: transformacion_etl.py

### 2.2 Transformaciones Aplicadas

**A. Carga de Datos**
- Lectura de CSV (productos, ventas)
- Lectura de JSON (clientes)
- Conversión a DataFrames de Pandas

**B. Agregación de Metadatos**
- fecha_carga: Timestamp del procesamiento
- fuente_dato: Origen del registro (productos/ventas/clientes)

**C. Validación de Calidad Inicial**
- Valores nulos: 0 encontrados
- Registros duplicados: 0 encontrados
- Integridad de IDs: 100%

**D. JOIN de Datasets**

Paso 1: Ventas + Productos
```
df_ventas_productos = df_ventas.merge(
    df_productos[['producto_id', 'nombre', 'categoria', 'precio']],
    on='producto_id',
    how='left'
)
```

Paso 2: Resultado + Clientes
```
df_consolidado = df_ventas_productos.merge(
    df_clientes[['cliente_id', 'nombre', 'apellido', 'ciudad', 'es_premium']],
    on='cliente_id',
    how='left'
)
```

**E. Cálculo de Métricas**
- diferencia_precio: Comparación precio venta vs catálogo
- descuento_valor: Descuento en valor absoluto
- categoria_venta: Categorización por monto (Baja/Media/Alta)

**F. Renombramiento de Columnas**
- nombre → nombre_producto
- precio → precio_catalogo
- nombre_cliente (del merge)

### 2.3 Resultado de Transformación

Dataset consolidado final:
- Registros: 500
- Columnas: 22
- Formato: CSV + SQLite

Columnas incluidas:
- Datos de venta: venta_id, fecha, hora, cantidad, total
- Datos de producto: producto_id, nombre_producto, categoria, precio_catalogo
- Datos de cliente: cliente_id, nombre_cliente, apellido, ciudad, es_premium
- Metadatos: fecha_carga, fuente_dato
- Métricas calculadas: diferencia_precio, descuento_valor, categoria_venta

---

## 💾 FASE 3: CARGA (Load)

### 3.1 Destino de Datos

Base de datos SQLite: tienda_online.db

### 3.2 Tablas Creadas

**Tabla: productos**
- Registros: 50
- Contenido: Catálogo de productos con metadatos

**Tabla: ventas**
- Registros: 500
- Contenido: Transacciones originales con metadatos

**Tabla: clientes**
- Registros: 100
- Contenido: Base de clientes con metadatos

**Tabla: ventas_consolidadas**
- Registros: 500
- Contenido: Dataset unificado con JOIN de las 3 fuentes
- Uso: Análisis de negocio y reporting

### 3.3 Archivo Exportado

ventas_consolidadas_final.csv
- Formato: CSV UTF-8
- Tamaño: 500 registros × 22 columnas
- Propósito: Backup y análisis externo

---

## ✅ FASE 4: VALIDACIÓN

### 4.1 Script de Validación

Archivo: validacion_etl.py

### 4.2 Validaciones Realizadas

**1. Integridad Referencial**
- Productos huérfanos: 0
- Clientes huérfanos: 0
- Score: 100%

**2. Consistencia de Datos**
- Totales incorrectos: 10 (diferencias de redondeo ±0.01€)
- Valores negativos: 0
- Valores inválidos: 0
- Score: 50%

**3. Completitud**
- Valores nulos en campos críticos: 0
- Registros duplicados: 0
- Score: 100%

**4. Transformaciones**
- Metadatos agregados: 100%
- JOIN completado correctamente: 100%
- Métricas calculadas: 100%

### 4.3 Score de Calidad Total

**87.5% - BUENO**

Desglose:
- Integridad Referencial: 100%
- Consistencia de Datos: 50% (redondeo)
- Completitud: 100%
- Sin Duplicados: 100%

**Conclusión:** El proceso ETL tiene excelente calidad. Las pequeñas diferencias de redondeo (±1 céntimo) son esperadas y aceptables en sistemas reales.

### 4.4 Reporte de Validación

Archivo generado: reporte_validacion.json

---

## 📊 RESULTADOS DEL NEGOCIO

### Métricas Principales

- Total de ventas: 500 transacciones
- Ingresos totales: €44,184.64
- Ticket promedio: €88.37
- Clientes únicos: 100
- Productos vendidos: 50

### Ventas por Categoría

| Categoría | Número de Ventas | Ingresos | Ticket Promedio |
|-----------|------------------|----------|-----------------|
| Electrónica | 87 | €10,897.78 | €125.26 |
| Hogar | 125 | €10,781.52 | €86.25 |
| Ropa | 107 | €10,399.33 | €97.19 |
| Libros | 98 | €6,251.36 | €63.79 |
| Deportes | 83 | €5,854.65 | €70.54 |

### Insights

1. Electrónica tiene el ticket promedio más alto (€125.26)
2. Hogar es la categoría con más transacciones (125)
3. Libros tiene el ticket promedio más bajo (€63.79)
4. 100% de los clientes en la base han realizado al menos una compra

---

## 📁 Estructura del Proyecto
```
Reto-7-ETL-Procesos/
├── README.md (esta documentación)
├── generar_datasets_tienda.py
├── transformacion_etl.py
├── validacion_etl.py
├── productos.csv
├── ventas.csv
├── clientes.json
├── flujo_nifi_extraccion.json
├── flujo_nifi_extraccion.png
└── data_warehouse/
    ├── productos/
    │   └── productos.csv
    ├── ventas/
    │   └── ventas.csv
    ├── clientes/
    │   └── clientes.json
    ├── tienda_online.db
    ├── ventas_consolidadas_final.csv
    └── reporte_validacion.json
```

---

## 🚀 Cómo Ejecutar el Proyecto

### Requisitos Previos

1. Python 3.12+
2. Apache NiFi 2.8.0
3. Librerías: pandas, numpy

### Paso 1: Generar Datasets
```bash
python generar_datasets_tienda.py
```

Genera: productos.csv, ventas.csv, clientes.json

### Paso 2: Ejecutar Flujo NiFi

1. Iniciar Apache NiFi
2. Importar flujo_nifi_extraccion.json
3. Configurar rutas de directorios
4. Iniciar procesadores

### Paso 3: Transformación
```bash
python transformacion_etl.py
```

Genera: tienda_online.db, ventas_consolidadas_final.csv

### Paso 4: Validación
```bash
python validacion_etl.py
```

Genera: reporte_validacion.json

---

## 📝 Lecciones Aprendidas

### Técnicas

1. **Enfoque Híbrido:** Combinar herramientas especializadas (NiFi para orquestación, Python para transformaciones complejas) es más eficiente que usar una sola herramienta.

2. **Validación Temprana:** Validar datos en cada etapa del proceso evita propagar errores.

3. **Metadatos:** Agregar información de procesamiento (fecha_carga, fuente_dato) facilita auditoría y debugging.

4. **Estructura de Datos:** Organizar archivos en carpetas tipo Data Warehouse mejora la mantenibilidad.

### Desafíos

1. **Configuración de NiFi:** La configuración inicial de Controller Services requiere entender bien el modelo de procesamiento de NiFi.

2. **JOIN de Datasets:** Gestionar sufijos de columnas en merges de Pandas requiere atención al detalle.

3. **Redondeo de Decimales:** Las diferencias de redondeo son inevitables y deben ser aceptadas dentro de márgenes razonables.

---

## 🔄 Mejoras Futuras

1. **Automatización:** Programar ejecución automática del flujo ETL (cron jobs, NiFi scheduling)

2. **Escalabilidad:** Migrar a Apache Spark para procesar volúmenes mayores de datos

3. **Monitoreo:** Implementar alertas y dashboards para monitorear la calidad del ETL en tiempo real

4. **Versionado de Datos:** Implementar sistema de versionado para trackear cambios históricos

5. **Testing:** Crear suite de pruebas automatizadas para validar transformaciones

---

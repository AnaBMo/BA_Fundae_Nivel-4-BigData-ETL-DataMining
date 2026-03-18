# 📊 RETO 6 - OPEN DATA: Observatorio de Comercio Minorista España

**Proyecto:** Enriquecimiento de Dataset con Open Data del INE  
**Programa:** FUNDAE Business Analytics Nivel 4  
**Autor:** Ana BM  
**Fecha:** Marzo 2026  
**GitHub:** [BA_Fundae_Nivel-4-BigData-ETL-DataMining](https://github.com/AnaBMo/BA_Fundae_Nivel-4-BigData-ETL-DataMining)

---

## 🎯 Objetivo del Proyecto

Demostrar capacidades de **integración de datos abiertos** mediante el enriquecimiento de un dataset de transacciones comerciales con indicadores económicos oficiales del Instituto Nacional de Estadística (INE), aplicando transformaciones complejas y validaciones rigurosas.

---

## 📋 Tabla de Contenidos

1. [Resumen Ejecutivo](#-resumen-ejecutivo)
2. [Datos Utilizados](#-datos-utilizados)
3. [Metodología](#-metodología)
4. [Tecnologías y Habilidades](#-tecnologías-y-habilidades)
5. [Estructura del Proyecto](#-estructura-del-proyecto)
6. [Resultados](#-resultados)
7. [Conclusiones](#-conclusiones)
8. [Cómo Ejecutar](#-cómo-ejecutar)

---

## 🎉 Resumen Ejecutivo

### Logros Principales

- ✅ **99,457 transacciones** enriquecidas con Open Data oficial
- ✅ **504 registros** de indicadores IPC descargados vía API INE
- ✅ **100% de cobertura** en todas las variables integradas (0 nulos)
- ✅ **11 columnas nuevas** añadidas al dataset original
- ✅ **3 fuentes de datos** integradas mediante LEFT JOIN sin pérdida de registros
- ✅ **Metodología reproducible** documentada en 3 notebooks

### Transformaciones Aplicadas

| Transformación | Descripción | Resultado |
|----------------|-------------|-----------|
| **Mapeo Geográfico** | Istanbul → España | 10 malls → 10 ciudades → 8 CCAA |
| **Mapeo Categorías** | Dataset → ECOICOP | 8 categorías → 5 grupos ECOICOP |
| **Integración IPC** | LEFT JOIN temporal | 3 indicadores IPC (Nacional, Categoría, CCAA) |

---

## 📊 Datos Utilizados

### 1. Dataset Principal
**Fuente:** [Kaggle - Customer Shopping Dataset Istanbul](https://www.kaggle.com/datasets/mehmettahiraslan/customer-shopping-dataset)

- **Registros:** 99,457 transacciones
- **Período:** 2021-01-01 a 2023-03-08 (27 meses)
- **Campos:** invoice_no, customer_id, gender, age, category, quantity, price, payment_method, invoice_date, shopping_mall
- **Calidad:** 0 nulos, 0 duplicados

**Distribución:**
- 10 centros comerciales
- 8 categorías de productos
- Rango edad: 18-69 años
- Género: 59.81% Mujeres, 40.19% Hombres

### 2. Open Data INE

#### 2.1 IPC Nacional
- **Fuente:** API INE - Tabla 50902
- **Registros:** 36 meses (2021-2023) 
- **Variable:** Índice de Precios al Consumo General
- **Rango:** 97.01 - 111.77

#### 2.2 IPC por Categorías ECOICOP
- **Fuente:** API INE - Tabla 50902
- **Registros:** 180 (5 categorías × 36 meses)
- **Categorías:** 
  - 01: Alimentos y bebidas no alcohólicas
  - 03: Vestido y calzado
  - 08: Comunicaciones
  - 09: Ocio y cultura
  - 12: Otros bienes y servicios
- **Rango:** 90.48 - 123.47

#### 2.3 IPC por Comunidades Autónomas
- **Fuente:** API INE - Tabla 50913
- **Registros:** 288 (8 CCAA × 36 meses)
- **CCAA:** Andalucía, Aragón, Baleares, Canarias, Cataluña, C. Valenciana, Madrid, País Vasco
- **Rango:** 96.69 - 112.59

### 3. Archivos de Mapeo (Creación Propia)

#### 3.1 Mapeo Geográfico
Conversión de shopping malls de Istanbul a ciudades españolas equivalentes por volumen poblacional y características comerciales.

| Shopping Mall (Istanbul) | Ciudad España | CCAA | Código CCAA |
|--------------------------|---------------|------|-------------|
| Mall of Istanbul | Madrid | Madrid | 13 |
| Kanyon | Barcelona | Cataluña | 09 |
| Metrocity | Valencia | Comunitat Valenciana | 10 |
| Metropol AVM | Sevilla | Andalucía | 01 |
| Istinye Park | Bilbao | País Vasco | 16 |
| Zorlu Center | Zaragoza | Aragón | 02 |
| Cevahir AVM | Málaga | Andalucía | 01 |
| Forum Istanbul | Alicante | Comunitat Valenciana | 10 |
| Viaport Outlet | Palma de Mallorca | Illes Balears | 04 |
| Emaar Square Mall | Las Palmas | Canarias | 05 |

#### 3.2 Mapeo Categorías
Conversión de categorías del dataset a grupos ECOICOP del INE.

| Categoría Dataset | Grupo ECOICOP | Código |
|-------------------|---------------|--------|
| Food & Beverage | Alimentos y bebidas no alcohólicas | 01 |
| Clothing | Vestido y calzado | 03 |
| Shoes | Vestido y calzado | 03 |
| Technology | Comunicaciones | 08 |
| Books | Ocio y cultura | 09 |
| Toys | Ocio y cultura | 09 |
| Cosmetics | Otros bienes y servicios | 12 |
| Souvenir | Otros bienes y servicios | 12 |

---

## 🔬 Metodología

### Fase 1: Exploración y Preparación

**Notebook 01 - Exploración Dataset Principal**
1. Carga y análisis exploratorio
2. Verificación de calidad (nulos, duplicados, tipos de datos)
3. Análisis de distribuciones (temporal, demográfica, categórica)
4. Identificación de variables clave para integración

### Fase 2: Descarga Open Data

**Notebook 02 - Descarga Open Data INE**
1. Conexión a APIs REST del INE
2. Descarga de 3 fuentes de datos:
   - IPC Nacional (tabla 50902)
   - IPC Categorías (tabla 50902)
   - IPC CCAA (tabla 50913)
3. Procesamiento de respuestas JSON
4. Validación de cobertura temporal
5. Guardado de archivos CSV

**Desafíos resueltos:**
- Parsing JSON → dict Python (error inicial con índice [0])
- Ajuste de parámetro `nult` para cobertura completa 2021-2023
- Identificación de series correctas en tablas con múltiples indicadores

### Fase 3: Integración y Enriquecimiento

**Notebook 03 - Preparación e Integración**
1. **Aplicación de mapeos:**
   - Geográfico: Istanbul → España (10 malls → 10 ciudades → 8 CCAA)
   - Categorías: Dataset → ECOICOP (8 categorías → 5 grupos)

2. **Preparación temporal:**
   - Extracción de year y month
   - Creación de fecha_mes normalizada (primer día del mes)
   - Sincronización de fechas entre datasets

3. **Integración mediante LEFT JOIN:**
   - JOIN 1: Dataset + IPC Nacional (por fecha_mes)
   - JOIN 2: Dataset + IPC Categoría (por fecha_mes + codigo_ecoicop)
   - JOIN 3: Dataset + IPC CCAA (por fecha_mes + codigo_ccaa)

4. **Validación:**
   - Verificación de registros pre/post JOIN
   - Cálculo de porcentajes de cobertura
   - Estadísticas descriptivas de variables IPC

---

## 💻 Tecnologías y Habilidades

### Python & Pandas
- ✅ Manipulación de DataFrames complejos
- ✅ Merge/JOIN de múltiples datasets
- ✅ Transformación de tipos de datos
- ✅ Manejo de fechas y timestamps
- ✅ Validación de calidad de datos

### APIs & Web
- ✅ Peticiones HTTP con `requests`
- ✅ Parsing JSON → dict Python
- ✅ Manejo de parámetros API (`nult`)
- ✅ Troubleshooting de errores API
- ✅ Verificación de tipos de datos

### Datos Abiertos
- ✅ Acceso a APIs INE (3 tablas diferentes)
- ✅ Integración multi-fuente
- ✅ Mapeos personalizados
- ✅ Validación de cobertura temporal

### Metodología
- ✅ Trabajo incremental (notebook por notebook)
- ✅ Validaciones en cada paso
- ✅ Documentación detallada
- ✅ Control de versiones (Drive + GitHub)

### Herramientas
- **Python 3.8+**
- **Pandas 2.0+**
- **Google Colab** (entorno de ejecución)
- **Google Drive** (almacenamiento)
- **Git/GitHub** (control de versiones)

---

## 📁 Estructura del Proyecto

```
Reto-6-Open-Data/
│
├── README.md                          # Este archivo
│
├── data/
│   ├── raw/                           # Datos originales
│   │   ├── customer_shopping_data.csv    # Dataset principal (99,457 registros)
│   │   ├── ine_ipc_nacional.csv          # IPC Nacional (36 meses)
│   │   ├── ine_ipc_categorias.csv        # IPC Categorías (180 registros)
│   │   └── ine_ipc_ccaa.csv              # IPC CCAA (288 registros)
│   │
│   ├── mapping/                       # Archivos de mapeo
│   │   ├── istanbul_spain_mapping.csv    # Mapeo geográfico (10 malls)
│   │   └── categories_ecoicop_mapping.csv # Mapeo categorías (8 categorías)
│   │
│   └── enriched/                      # Dataset final
│       └── dataset_enriquecido_final.csv # 99,457 × 21 columnas
│
└── notebooks/                         # Notebooks Jupyter
    ├── 01_exploracion_dataset_principal.ipynb
    ├── 02_descarga_open_data_ine.ipynb
    └── 03_preparacion_integracion_datasets.ipynb
```

---

## 📈 Resultados

### Dataset Enriquecido Final

**Archivo:** `dataset_enriquecido_final.csv`

**Dimensiones:**
- **Registros:** 99,457 (sin pérdida)
- **Columnas:** 21 (10 originales + 11 nuevas)
- **Tamaño:** 15.67 MB

**Columnas Originales:**
1. `invoice_no` - Número de factura
2. `customer_id` - ID del cliente
3. `gender` - Género
4. `age` - Edad
5. `category` - Categoría original
6. `quantity` - Cantidad
7. `price` - Precio
8. `payment_method` - Método de pago
9. `invoice_date` - Fecha de factura
10. `shopping_mall` - Centro comercial

**Columnas Añadidas:**
11. `ciudad_espana` - Ciudad española mapeada
12. `ccaa` - Comunidad Autónoma
13. `codigo_ccaa` - Código CCAA (01-16)
14. `codigo_ecoicop` - Código grupo ECOICOP (1-12)
15. `grupo_ecoicop` - Nombre del grupo ECOICOP
16. `year` - Año de la transacción
17. `month` - Mes de la transacción
18. `fecha_mes` - Fecha normalizada (primer día del mes)
19. `IPC_Nacional` - Índice de Precios al Consumo Nacional
20. `IPC_Categoria` - IPC por categoría ECOICOP
21. `IPC_CCAA` - IPC por Comunidad Autónoma

### Métricas de Calidad

| Métrica | Valor |
|---------|-------|
| **Cobertura IPC Nacional** | 100.00% |
| **Cobertura IPC Categoría** | 100.00% |
| **Cobertura IPC CCAA** | 100.00% |
| **Valores nulos en variables críticas** | 0 |
| **Registros perdidos en JOIN** | 0 |
| **Duplicados** | 0 |

### Estadísticas IPC

#### IPC Nacional
- **Media:** 105.26
- **Desviación estándar:** 4.74
- **Rango:** 97.01 - 111.77

#### IPC Categoría
- **Media:** 102.64
- **Desviación estándar:** 6.15
- **Rango:** 90.48 - 123.47

#### IPC CCAA
- **Media:** 105.11
- **Desviación estándar:** 4.62
- **Rango:** 96.69 - 112.59

### Distribución por Ciudad

| Ciudad | Transacciones | % |
|--------|---------------|---|
| Madrid | 19,943 | 20.05% |
| Barcelona | 19,823 | 19.93% |
| Valencia | 15,011 | 15.09% |
| Sevilla | 10,161 | 10.22% |
| Bilbao | 9,781 | 9.83% |
| Otras | 24,738 | 24.87% |

### Distribución por Grupo ECOICOP

| Grupo ECOICOP | Transacciones | % |
|---------------|---------------|---|
| Vestido y calzado | 44,521 | 44.76% |
| Otros bienes y servicios | 20,096 | 20.21% |
| Ocio y cultura | 15,068 | 15.15% |
| Alimentos y bebidas | 14,776 | 14.86% |
| Comunicaciones | 4,996 | 5.02% |

---

## 🎯 Conclusiones

### Logros Técnicos

1. **Integración Multi-Fuente Exitosa**
   - Se integraron 4 fuentes de datos diferentes (1 principal + 3 Open Data)
   - LEFT JOIN preservó el 100% de registros originales
   - Cobertura perfecta en todas las variables IPC

2. **Mapeos Personalizados**
   - Creación de mapeos lógicos Istanbul → España
   - Conversión exitosa a clasificación ECOICOP del INE
   - Validación de coherencia en categorías

3. **Calidad de Datos**
   - 0 valores nulos en variables críticas
   - 0 duplicados en dataset final
   - Validaciones rigurosas en cada etapa

### Aprendizajes Clave

1. **APIs REST del INE**
   - Comprensión de estructura JSON de respuestas
   - Manejo de parámetros de consulta (`nult`)
   - Diferencia entre tablas y series individuales

2. **Integración Temporal**
   - Importancia de normalizar fechas al primer día del mes
   - Uso de `to_period('M').to_timestamp()` para sincronización
   - Validación de cobertura temporal antes de JOIN

3. **Transformaciones de Datos**
   - Conversión de tipos de datos (str → int, timestamp → date)
   - Manejo de códigos con ceros a la izquierda (`str.zfill(2)`)
   - Creación de variables derivadas (year, month, fecha_mes)

### Aplicaciones Potenciales

Este dataset enriquecido permite análisis avanzados como:

- **Análisis de inflación por categoría:** Comparar evolución de precios vs IPC
- **Segmentación geográfica:** Identificar diferencias regionales en comportamiento
- **Tendencias temporales:** Analizar estacionalidad ajustada por inflación
- **Poder adquisitivo:** Calcular precios reales ajustados por IPC
- **Comparativas CCAA:** Detectar patrones de consumo por región

---

## 🚀 Cómo Ejecutar

### Prerrequisitos

```bash
# Librerías necesarias
pip install pandas numpy matplotlib seaborn requests
```

### Ejecución en Google Colab

1. **Clonar el repositorio:**
```bash
git clone https://github.com/AnaBMo/BA_Fundae_Nivel-4-BigData-ETL-DataMining.git
```

2. **Subir notebooks a Google Colab**

3. **Montar Google Drive y crear estructura de carpetas:**
```python
from google.colab import drive
drive.mount('/content/drive')

# Crear estructura
import os
BASE_PATH = '/content/drive/MyDrive/Reto-6-Open-Data/'
os.makedirs(BASE_PATH + 'data/raw', exist_ok=True)
os.makedirs(BASE_PATH + 'data/mapping', exist_ok=True)
os.makedirs(BASE_PATH + 'data/enriched', exist_ok=True)
```

4. **Ejecutar notebooks en orden:**
   - `01_exploracion_dataset_principal.ipynb`
   - `02_descarga_open_data_ine.ipynb`
   - `03_preparacion_integracion_datasets.ipynb`

### Orden de Ejecución

```
Notebook 01 → Exploración
    ↓
Notebook 02 → Descarga Open Data (genera 3 CSV)
    ↓
Notebook 03 → Integración (genera dataset_enriquecido_final.csv)
```

---

## 📚 Referencias

### Fuentes de Datos

- **Dataset Principal:** [Kaggle - Customer Shopping Dataset](https://www.kaggle.com/datasets/mehmettahiraslan/customer-shopping-dataset)
- **INE - Índice de Precios al Consumo:** [ine.es](https://www.ine.es)
- **API INE - Documentación:** [ine.es/dyngs/DataLab/manual.html](https://www.ine.es/dyngs/DataLab/manual.html)

### Clasificaciones

- **ECOICOP (European Classification of Individual Consumption by Purpose):** Clasificación europea de consumo individual por finalidad utilizada por el INE para el IPC.

### Tecnologías

- **Pandas:** [pandas.pydata.org](https://pandas.pydata.org)
- **Requests:** [docs.python-requests.org](https://docs.python-requests.org)
- **Google Colab:** [colab.research.google.com](https://colab.research.google.com)

---

## 📄 Licencia

Este proyecto es parte del programa educativo FUNDAE Business Analytics Nivel 4.

Los datos del INE son de acceso público bajo la política de datos abiertos del organismo.

# 🔍 EJERCICIOS DE VALIDACIÓN MANUAL - RETO 5 HADOOP

## 📋 **Objetivo**
Verificar manualmente que los resultados procesados por Apache Hadoop/Hive coinciden exactamente con los datos crudos originales. Concepto: **Bronze Layer vs Gold Layer Validation**.

---

## 🥉 **BRONZE LAYER - Datos Crudos**

**Archivo:** `ventas_mercado_especias_expanded.csv`  
**Registros:** 25,225 transacciones individuales  
**Formato:** Datos sin procesar, una fila por transacción

---

## 🥇 **GOLD LAYER - Datos Procesados**

**Archivos:** Resultados de consultas Hive  
**Registros:** Datos agregados y sumarizados  
**Formato:** Conteos, promedios, agrupaciones

---

## ✅ **EJERCICIO 1: Validar Top Productos**

### **Método Manual:**
```bash
# 1. Contar "Americano with Milk" en el CSV
grep "Americano with Milk" ventas_mercado_especias_expanded.csv | wc -l

# 2. Contar "Latte" en el CSV  
grep "Latte" ventas_mercado_especias_expanded.csv | wc -l

# 3. Contar "Americano" (exacto, no "with Milk")
grep "^[^,]*,[^,]*,[^,]*,[^,]*,[^,]*,Americano," ventas_mercado_especias_expanded.csv | wc -l
```

### **Resultados Esperados (Gold Layer):**
- **Americano with Milk:** 5,722 ventas
- **Latte:** 5,473 ventas
- **Americano:** 4,000 ventas

### **Verificación:**
- [ ] ¿Coincide el conteo manual con resultados_hadoop_limpio.csv?
- [ ] ¿La suma de los top 8 productos coincide con 25,225?

---

## ✅ **EJERCICIO 2: Validar Evolución Anual**

### **Método Manual:**
```bash
# Contar registros por año
grep "^2019" ventas_mercado_especias_expanded.csv | wc -l
grep "^2020" ventas_mercado_especias_expanded.csv | wc -l  
grep "^2021" ventas_mercado_especias_expanded.csv | wc -l
grep "^2022" ventas_mercado_especias_expanded.csv | wc -l
grep "^2023" ventas_mercado_especias_expanded.csv | wc -l
grep "^2024" ventas_mercado_especias_expanded.csv | wc -l
grep "^2025" ventas_mercado_especias_expanded.csv | wc -l
```

### **Resultados Esperados (Gold Layer):**
- **2019:** 3,953 transacciones
- **2020:** 3,522 transacciones
- **2021:** 3,749 transacciones  
- **2022:** 2,881 transacciones
- **2023:** 3,668 transacciones
- **2024:** 6,509 transacciones
- **2025:** 943 transacciones

### **Verificación:**
- [ ] ¿Cada año coincide con evolucion_anual.txt?
- [ ] ¿La suma total es 25,225?
- [ ] ¿2024 efectivamente tiene el mayor crecimiento?

---

## ✅ **EJERCICIO 3: Validar Análisis Estacional**

### **Método Manual:**
```bash
# Contar por temporada
grep ",Primavera," ventas_mercado_especias_expanded.csv | wc -l
grep ",Otoño," ventas_mercado_especias_expanded.csv | wc -l
grep ",Verano," ventas_mercado_especias_expanded.csv | wc -l  
grep ",Invierno," ventas_mercado_especias_expanded.csv | wc -l
```

### **Cálculo de Precios Promedio:**
```bash
# Extraer precios de Primavera y calcular promedio
grep ",Primavera," ventas_mercado_especias_expanded.csv | cut -d',' -f5 > precios_primavera.txt
# Usar calculadora o Excel para promediar
```

### **Resultados Esperados (Gold Layer):**
- **Primavera:** 6,463 ventas - €31.59 promedio
- **Otoño:** 6,377 ventas - €31.37 promedio
- **Verano:** 6,241 ventas - €31.21 promedio
- **Invierno:** 6,144 ventas - €31.31 promedio

### **Verificación:**
- [ ] ¿Las cantidades por temporada coinciden con analisis_temporadas.txt?
- [ ] ¿Los precios promedio están dentro de ±€0.05?
- [ ] ¿La distribución es equilibrada (~25% cada temporada)?

---

## ✅ **EJERCICIO 4: Validar Tipos de Pago**

### **Método Manual:**
```bash
# Contar transacciones por tipo de pago
grep ",card," ventas_mercado_especias_expanded.csv | wc -l
grep ",cash," ventas_mercado_especias_expanded.csv | wc -l
```

### **Resultados Esperados (Gold Layer):**
- **Tarjeta (card):** 21,899 transacciones - €31.36 promedio
- **Efectivo (cash):** 3,326 transacciones - €31.42 promedio

### **Verificación:**
- [ ] ¿87% tarjeta vs 13% efectivo aproximadamente?
- [ ] ¿Los tickets promedio son similares (~€31.40)?
- [ ] ¿La suma es exactamente 25,225?

---

## 📊 **EJERCICIO 5: Validación Cruzada**

### **Verificaciones de Integridad:**
```bash
# Total de líneas del CSV (debería ser 25,226 con header)
wc -l ventas_mercado_especias_expanded.csv

# Verificar que no hay líneas vacías
grep -c "^$" ventas_mercado_especias_expanded.csv

# Verificar formato de fechas
head -10 ventas_mercado_especias_expanded.csv
```

### **Preguntas de Control:**
- [ ] ¿Todas las fechas están en formato YYYY-MM-DD?
- [ ] ¿Todos los precios son números positivos?
- [ ] ¿Los nombres de productos coinciden con los esperados?
- [ ] ¿Las temporadas solo contienen: Primavera, Verano, Otoño, Invierno?

---

## 🎯 **PLANTILLA DE RESULTADOS**

### **Tabla de Validación:**
| Métrica | Bronze (Manual) | Gold (Hadoop) | ¿Coincide? | Diferencia |
|---------|-----------------|---------------|------------|------------|
| Total registros | | 25,225 | | |
| Americano with Milk | | 5,722 | | |
| Latte | | 5,473 | | |
| Año 2024 | | 6,509 | | |
| Primavera | | 6,463 | | |
| Tarjeta | | 21,899 | | |

### **Conclusiones:**
- **Precisión global:** ____%
- **Discrepancias encontradas:** ___
- **Calificación:** ⭐⭐⭐⭐⭐ (EXCELENTE) / ⭐⭐⭐⭐ (BUENO) / ⭐⭐⭐ (REVISAR)

---

## 💡 **Comandos de Utilidad**

### **Para contar líneas específicas:**
```bash
grep "PATRON" archivo.csv | wc -l
```

### **Para extraer columnas específicas:**
```bash
cut -d',' -f5 archivo.csv  # Columna 5 (precio)
```

### **Para ordenar y contar únicos:**
```bash
cut -d',' -f6 archivo.csv | sort | uniq -c  # Contar productos únicos
```

---

## 🎓 **Valor Educativo**

Este ejercicio demuestra:
- **Integridad de datos** - Los procesamientos Big Data son fieles al origen
- **Comprensión profunda** - Entiendes tanto Bronze como Gold layers
- **Validación técnica** - Capacidad de verificar resultados analíticos
- **Rigor profesional** - Siempre validar antes de presentar insights

-- =============================================================================
-- RETO 5 - CONSULTAS HIVE BIG DATA
-- Proyecto: Análisis del Mercado de las Especias con Hadoop
-- Tecnología: Apache Hive 3.1 sobre Google Cloud Dataproc
-- =============================================================================

-- -----------------------------------------------------------------------------
-- 1. CREACIÓN DE TABLAS
-- -----------------------------------------------------------------------------

-- Tabla principal (todos los datos - mixtos)
CREATE TABLE ventas_cafe (
  fecha STRING,
  datetime STRING,
  tipo_pago STRING,
  tarjeta STRING,
  precio DOUBLE,
  nombre_cafe STRING,
  es_outlier STRING,
  mes INT,
  ano INT,
  dia_semana INT,
  es_fin_semana STRING,
  temporada STRING
) 
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY ',' 
LOCATION '/mercado-especias/datos/';

-- Tabla solo ventas (datos limpios)
CREATE TABLE solo_ventas (
  fecha STRING,
  datetime STRING,
  tipo_pago STRING,
  tarjeta STRING,
  precio DOUBLE,
  nombre_cafe STRING,
  es_outlier STRING,
  mes INT,
  ano INT,
  dia_semana INT,
  es_fin_semana STRING,
  temporada STRING
) 
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY ',' 
LOCATION '/mercado-especias/solo-ventas/'
TBLPROPERTIES ("skip.header.line.count"="1");

-- -----------------------------------------------------------------------------
-- 2. CONSULTAS DE EXPLORACIÓN
-- -----------------------------------------------------------------------------

-- Ver estructura de las tablas
SHOW TABLES;

-- Primeros registros para verificar datos
SELECT * FROM solo_ventas LIMIT 5;

-- Conteo total de registros
SELECT COUNT(*) as total_registros FROM solo_ventas;

-- -----------------------------------------------------------------------------
-- 3. ANÁLISIS PRINCIPAL: TOP PRODUCTOS
-- -----------------------------------------------------------------------------

-- Top cafés más vendidos
SELECT nombre_cafe, COUNT(*) as total_ventas 
FROM solo_ventas 
GROUP BY nombre_cafe 
ORDER BY total_ventas DESC 
LIMIT 10;

-- -----------------------------------------------------------------------------
-- 4. ANÁLISIS TEMPORAL Y ESTACIONAL
-- -----------------------------------------------------------------------------

-- Ventas por temporada con precio promedio
SELECT temporada, COUNT(*) as ventas, ROUND(AVG(precio), 2) as precio_promedio
FROM solo_ventas 
GROUP BY temporada 
ORDER BY ventas DESC;

-- Evolución anual de ventas
SELECT ano, COUNT(*) as ventas_anuales
FROM solo_ventas 
GROUP BY ano 
ORDER BY ano;

-- Ventas por mes (análisis estacional detallado)
SELECT mes, COUNT(*) as ventas_mensuales, ROUND(AVG(precio), 2) as precio_promedio
FROM solo_ventas 
GROUP BY mes 
ORDER BY mes;

-- -----------------------------------------------------------------------------
-- 5. ANÁLISIS DE PRECIOS
-- -----------------------------------------------------------------------------

-- Estadísticas de precios por producto
SELECT nombre_cafe, 
       COUNT(*) as total_ventas,
       ROUND(AVG(precio), 2) as precio_promedio,
       ROUND(MIN(precio), 2) as precio_minimo,
       ROUND(MAX(precio), 2) as precio_maximo
FROM solo_ventas 
GROUP BY nombre_cafe 
ORDER BY precio_promedio DESC;

-- Análisis por tipo de pago
SELECT tipo_pago, COUNT(*) as total_transacciones,
       ROUND(AVG(precio), 2) as ticket_promedio
FROM solo_ventas 
GROUP BY tipo_pago;

-- -----------------------------------------------------------------------------
-- 6. ANÁLISIS DE COMPORTAMIENTO
-- -----------------------------------------------------------------------------

-- Ventas por día de la semana (0=domingo, 6=sábado)
SELECT dia_semana, COUNT(*) as total_ventas,
       CASE 
         WHEN dia_semana = 0 THEN 'Domingo'
         WHEN dia_semana = 1 THEN 'Lunes'
         WHEN dia_semana = 2 THEN 'Martes'
         WHEN dia_semana = 3 THEN 'Miércoles'
         WHEN dia_semana = 4 THEN 'Jueves'
         WHEN dia_semana = 5 THEN 'Viernes'
         WHEN dia_semana = 6 THEN 'Sábado'
       END as nombre_dia
FROM solo_ventas 
GROUP BY dia_semana 
ORDER BY dia_semana;

-- Comparación fin de semana vs días laborables
SELECT es_fin_semana, COUNT(*) as total_ventas,
       ROUND(AVG(precio), 2) as ticket_promedio
FROM solo_ventas 
GROUP BY es_fin_semana;

-- -----------------------------------------------------------------------------
-- 7. TABLA DE RESULTADOS PARA EXPORTACIÓN
-- -----------------------------------------------------------------------------

-- Crear tabla con resultados principales para exportar
CREATE TABLE resultados_analisis AS
SELECT 'Top Cafes' as analisis, nombre_cafe as categoria, CAST(COUNT(*) AS STRING) as valor
FROM solo_ventas 
GROUP BY nombre_cafe 
ORDER BY COUNT(*) DESC 
LIMIT 10;

-- Ver resultados creados
SELECT * FROM resultados_analisis;

-- -----------------------------------------------------------------------------
-- 8. CONSULTAS DE VALIDACIÓN Y LIMPIEZA
-- -----------------------------------------------------------------------------

-- Verificar datos atípicos (outliers)
SELECT es_outlier, COUNT(*) as cantidad
FROM solo_ventas 
GROUP BY es_outlier;

-- Verificar rangos de precios
SELECT COUNT(*) as registros_precio_cero FROM solo_ventas WHERE precio = 0;
SELECT COUNT(*) as registros_precio_negativo FROM solo_ventas WHERE precio < 0;
SELECT COUNT(*) as registros_precio_muy_alto FROM solo_ventas WHERE precio > 100;

-- Productos con nombres inusuales (posibles errores)
SELECT nombre_cafe, COUNT(*) as frecuencia 
FROM solo_ventas 
GROUP BY nombre_cafe 
HAVING COUNT(*) < 100
ORDER BY frecuencia DESC;

-- -----------------------------------------------------------------------------
-- NOTAS TÉCNICAS:
-- - Total registros procesados: 25,225 transacciones
-- - Periodo: 2019-2025 (6 años de datos históricos)
-- - Tecnología: Apache Hive 3.1 con Tez como motor de ejecución
-- - Infraestructura: Google Cloud Dataproc (1 maestro + 2 trabajadores)
-- - Rendimiento: Consultas complejas ejecutadas en 7-13 segundos
-- =============================================================================

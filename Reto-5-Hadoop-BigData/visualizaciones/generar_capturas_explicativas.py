#!/usr/bin/env python3
"""
Generador de Capturas Explicativas - RETO 5 Hadoop
Crea diagramas profesionales del proceso técnico realizado
"""

import matplotlib.pyplot as plt
import matplotlib.patches as patches
from matplotlib.patches import FancyBboxPatch, Rectangle, Circle
import numpy as np

def crear_diagrama_arquitectura():
    """Crea diagrama de arquitectura del clúster Hadoop"""
    fig, ax = plt.subplots(1, 1, figsize=(14, 10))
    
    # Colores profesionales
    color_google = '#4285F4'
    color_hadoop = '#FF6D01'
    color_hive = '#FDCC52'
    color_datos = '#34A853'
    
    # Título
    ax.text(0.5, 0.95, 'ARQUITECTURA CLÚSTER HADOOP - GOOGLE CLOUD DATAPROC', 
            ha='center', va='center', fontsize=16, fontweight='bold',
            transform=ax.transAxes)
    
    # Google Cloud (marco exterior)
    google_box = FancyBboxPatch((0.05, 0.1), 0.9, 0.8, 
                               boxstyle="round,pad=0.02",
                               facecolor='lightblue', alpha=0.3,
                               edgecolor=color_google, linewidth=2)
    ax.add_patch(google_box)
    ax.text(0.1, 0.85, 'GOOGLE CLOUD PLATFORM', fontsize=12, fontweight='bold',
            color=color_google, transform=ax.transAxes)
    
    # Nodo Maestro
    maestro = FancyBboxPatch((0.1, 0.65), 0.25, 0.2,
                            boxstyle="round,pad=0.01",
                            facecolor='orange', alpha=0.7,
                            edgecolor=color_hadoop, linewidth=2)
    ax.add_patch(maestro)
    ax.text(0.225, 0.78, 'NODO MAESTRO', ha='center', va='center', 
            fontweight='bold', fontsize=11, transform=ax.transAxes)
    ax.text(0.225, 0.72, 'hadoop-mercado-especias-m', ha='center', va='center',
            fontsize=9, transform=ax.transAxes)
    ax.text(0.225, 0.68, '• NameNode (HDFS)\n• ResourceManager\n• Hive Metastore',
            ha='center', va='center', fontsize=8, transform=ax.transAxes)
    
    # Nodos Trabajadores
    for i, pos_x in enumerate([0.45, 0.7]):
        trabajador = FancyBboxPatch((pos_x, 0.65), 0.2, 0.2,
                                   boxstyle="round,pad=0.01",
                                   facecolor='lightgreen', alpha=0.7,
                                   edgecolor=color_datos, linewidth=2)
        ax.add_patch(trabajador)
        ax.text(pos_x + 0.1, 0.78, f'TRABAJADOR {i+1}', ha='center', va='center',
                fontweight='bold', fontsize=10, transform=ax.transAxes)
        ax.text(pos_x + 0.1, 0.72, f'hadoop-mercado-especias-w-{i}',
                ha='center', va='center', fontsize=8, transform=ax.transAxes)
        ax.text(pos_x + 0.1, 0.68, '• DataNode\n• NodeManager\n• Procesamiento',
                ha='center', va='center', fontsize=8, transform=ax.transAxes)
    
    # HDFS Storage
    hdfs_box = FancyBboxPatch((0.1, 0.4), 0.8, 0.2,
                             boxstyle="round,pad=0.01",
                             facecolor='lightyellow', alpha=0.7,
                             edgecolor=color_hive, linewidth=2)
    ax.add_patch(hdfs_box)
    ax.text(0.5, 0.55, 'SISTEMA DE ARCHIVOS DISTRIBUIDO (HDFS)', 
            ha='center', va='center', fontweight='bold', fontsize=12,
            transform=ax.transAxes)
    
    # Datos en HDFS
    datos_info = [
        '/mercado-especias/datos/ (246K registros)',
        '/user/hive/warehouse/ (tablas Hive)',
        'Replicación: Factor 2 (cada archivo x2 copias)'
    ]
    
    for i, info in enumerate(datos_info):
        ax.text(0.5, 0.48 - i*0.03, f'• {info}', ha='center', va='center',
                fontsize=10, transform=ax.transAxes)
    
    # Herramientas
    tools_box = FancyBboxPatch((0.1, 0.15), 0.8, 0.2,
                              boxstyle="round,pad=0.01",
                              facecolor='lavender', alpha=0.7,
                              edgecolor='purple', linewidth=2)
    ax.add_patch(tools_box)
    ax.text(0.5, 0.3, 'HERRAMIENTAS DE PROCESAMIENTO', 
            ha='center', va='center', fontweight='bold', fontsize=12,
            transform=ax.transAxes)
    
    herramientas = [
        'Apache Hive 3.1 (SQL sobre Big Data)',
        'Apache Tez (Motor de ejecución optimizado)',  
        'YARN (Gestión de recursos y trabajos)'
    ]
    
    for i, tool in enumerate(herramientas):
        ax.text(0.5, 0.26 - i*0.03, f'• {tool}', ha='center', va='center',
                fontsize=10, transform=ax.transAxes)
    
    # Conexiones (flechas)
    # Maestro a trabajadores
    ax.annotate('', xy=(0.45, 0.75), xytext=(0.35, 0.75),
                arrowprops=dict(arrowstyle='<->', color='black', lw=2))
    ax.annotate('', xy=(0.7, 0.75), xytext=(0.35, 0.75),
                arrowprops=dict(arrowstyle='<->', color='black', lw=2))
    
    # Cluster a HDFS
    ax.annotate('', xy=(0.5, 0.6), xytext=(0.5, 0.65),
                arrowprops=dict(arrowstyle='<->', color='red', lw=2))
    
    ax.set_xlim(0, 1)
    ax.set_ylim(0, 1)
    ax.axis('off')
    
    plt.tight_layout()
    plt.savefig('arquitectura_cluster_hadoop.png', dpi=300, bbox_inches='tight')
    print("✅ Diagrama 'arquitectura_cluster_hadoop.png' generado")
    
    return fig

def crear_diagrama_proceso_mapreduce():
    """Crea diagrama del proceso MapReduce ejecutado"""
    fig, ax = plt.subplots(1, 1, figsize=(14, 10))
    
    # Título
    ax.text(0.5, 0.95, 'PROCESO MAPREDUCE - ANÁLISIS TOP CAFÉS', 
            ha='center', va='center', fontsize=16, fontweight='bold',
            transform=ax.transAxes)
    
    # Fase 1: INPUT (Datos originales)
    input_box = FancyBboxPatch((0.05, 0.8), 0.2, 0.1,
                              boxstyle="round,pad=0.01",
                              facecolor='lightblue', alpha=0.8)
    ax.add_patch(input_box)
    ax.text(0.15, 0.85, 'INPUT DATA', ha='center', va='center',
            fontweight='bold', fontsize=11, transform=ax.transAxes)
    ax.text(0.15, 0.82, '25,225 registros\nventas de café', ha='center', va='center',
            fontsize=9, transform=ax.transAxes)
    
    # Fase 2: MAP (División del trabajo)
    for i, y_pos in enumerate([0.75, 0.65, 0.55]):
        map_box = FancyBboxPatch((0.35, y_pos), 0.15, 0.08,
                                boxstyle="round,pad=0.01",
                                facecolor='lightgreen', alpha=0.8)
        ax.add_patch(map_box)
        ax.text(0.425, y_pos + 0.04, f'MAPPER {i+1}', ha='center', va='center',
                fontweight='bold', fontsize=10, transform=ax.transAxes)
        
        # Datos procesados por cada mapper
        registros = ['8,408 registros', '8,408 registros', '8,409 registros']
        ax.text(0.425, y_pos + 0.01, registros[i], ha='center', va='center',
                fontsize=8, transform=ax.transAxes)
    
    # Fase 3: SHUFFLE & SORT
    shuffle_box = FancyBboxPatch((0.55, 0.6), 0.15, 0.2,
                                boxstyle="round,pad=0.01",
                                facecolor='yellow', alpha=0.8)
    ax.add_patch(shuffle_box)
    ax.text(0.625, 0.72, 'SHUFFLE &\nSORT', ha='center', va='center',
            fontweight='bold', fontsize=11, transform=ax.transAxes)
    ax.text(0.625, 0.65, 'Agrupa por\nnombre_cafe', ha='center', va='center',
            fontsize=9, transform=ax.transAxes)
    
    # Fase 4: REDUCE (Agregación)
    reduce_box = FancyBboxPatch((0.75, 0.65), 0.15, 0.1,
                               boxstyle="round,pad=0.01",
                               facecolor='orange', alpha=0.8)
    ax.add_patch(reduce_box)
    ax.text(0.825, 0.7, 'REDUCER', ha='center', va='center',
            fontweight='bold', fontsize=11, transform=ax.transAxes)
    ax.text(0.825, 0.67, 'COUNT(*)\nGROUP BY', ha='center', va='center',
            fontsize=9, transform=ax.transAxes)
    
    # Fase 5: OUTPUT (Resultados)
    output_box = FancyBboxPatch((0.75, 0.45), 0.2, 0.15,
                               boxstyle="round,pad=0.01",
                               facecolor='lightcoral', alpha=0.8)
    ax.add_patch(output_box)
    ax.text(0.85, 0.55, 'RESULTADOS', ha='center', va='center',
            fontweight='bold', fontsize=11, transform=ax.transAxes)
    
    # Resultados reales
    resultados = [
        'Latte: 5,473',
        'Americano: 4,000',
        'Cappuccino: 3,534',
        '...'
    ]
    
    for i, resultado in enumerate(resultados):
        ax.text(0.85, 0.51 - i*0.02, resultado, ha='center', va='center',
                fontsize=8, transform=ax.transAxes)
    
    # Métricas de rendimiento
    metrics_box = FancyBboxPatch((0.05, 0.35), 0.9, 0.15,
                                boxstyle="round,pad=0.01",
                                facecolor='lightgray', alpha=0.5)
    ax.add_patch(metrics_box)
    ax.text(0.5, 0.46, 'MÉTRICAS DE RENDIMIENTO OBSERVADAS', 
            ha='center', va='center', fontweight='bold', fontsize=12,
            transform=ax.transAxes)
    
    metricas = [
        'Tiempo total: 13.9 segundos',
        'Vértices ejecutados: 3 (Map 1, Reducer 2, Reducer 3)',
        'Contenedores utilizados: 3',
        'Estado: SUCCEEDED (100% completado)'
    ]
    
    for i, metrica in enumerate(metricas):
        ax.text(0.5, 0.42 - i*0.025, f'• {metrica}', ha='center', va='center',
                fontsize=10, transform=ax.transAxes)
    
    # Consulta SQL ejecutada
    sql_box = FancyBboxPatch((0.05, 0.05), 0.9, 0.25,
                            boxstyle="round,pad=0.01",
                            facecolor='lightblue', alpha=0.3,
                            edgecolor='blue', linewidth=2)
    ax.add_patch(sql_box)
    ax.text(0.5, 0.25, 'CONSULTA HIVE EJECUTADA', 
            ha='center', va='center', fontweight='bold', fontsize=12,
            transform=ax.transAxes)
    
    sql_query = """SELECT nombre_cafe, COUNT(*) as total_ventas 
FROM solo_ventas 
GROUP BY nombre_cafe 
ORDER BY total_ventas DESC 
LIMIT 10;"""
    
    ax.text(0.5, 0.15, sql_query, ha='center', va='center',
            fontfamily='monospace', fontsize=10, 
            bbox=dict(boxstyle="round,pad=0.5", facecolor="white", alpha=0.8),
            transform=ax.transAxes)
    
    # Flechas del flujo
    flechas = [
        ((0.25, 0.85), (0.35, 0.79)),  # Input -> Map1
        ((0.25, 0.85), (0.35, 0.69)),  # Input -> Map2  
        ((0.25, 0.85), (0.35, 0.59)),  # Input -> Map3
        ((0.5, 0.7), (0.55, 0.7)),     # Maps -> Shuffle
        ((0.7, 0.7), (0.75, 0.7)),     # Shuffle -> Reduce
        ((0.825, 0.65), (0.825, 0.6))  # Reduce -> Output
    ]
    
    for start, end in flechas:
        ax.annotate('', xy=end, xytext=start,
                    arrowprops=dict(arrowstyle='->', color='red', lw=2))
    
    ax.set_xlim(0, 1)
    ax.set_ylim(0, 1)
    ax.axis('off')
    
    plt.tight_layout()
    plt.savefig('proceso_mapreduce_hadoop.png', dpi=300, bbox_inches='tight')
    print("✅ Diagrama 'proceso_mapreduce_hadoop.png' generado")
    
    return fig

def crear_cronologia_proyecto():
    """Crea línea de tiempo del proyecto"""
    fig, ax = plt.subplots(1, 1, figsize=(14, 8))
    
    # Título
    ax.text(0.5, 0.95, 'CRONOLOGÍA DEL PROYECTO - RETO 5 HADOOP', 
            ha='center', va='center', fontsize=16, fontweight='bold',
            transform=ax.transAxes)
    
    # Línea de tiempo principal
    ax.plot([0.1, 0.9], [0.5, 0.5], 'k-', linewidth=3)
    
    # Hitos del proyecto
    hitos = [
        (0.15, "CONFIGURACIÓN\nCLÚSTER", "• Google Cloud Dataproc\n• 3 nodos (1M + 2W)\n• Hadoop 3.3 + Hive 3.1"),
        (0.3, "GENERACIÓN\nDATOS", "• 246,700 registros\n• 5 años históricos\n• Python + Faker"),
        (0.45, "CARGA A\nHDFS", "• 22.4 MB distribuidos\n• Replicación factor 2\n• Cloud Storage → HDFS"),
        (0.6, "PROCESAMIENTO\nHIVE", "• Consultas SQL\n• MapReduce automático\n• Análisis en 7-13 seg"),
        (0.75, "EXPORTACIÓN\nRESULTADOS", "• Tablas → CSV\n• HDFS → Cloud Storage\n• Descarga local"),
        (0.9, "VISUALIZACIONES\n& PORTFOLIO", "• 5 gráficos PNG\n• Reporte HTML\n• Documentación")
    ]
    
    for x_pos, titulo, descripcion in hitos:
        # Punto en la línea
        ax.plot(x_pos, 0.5, 'ro', markersize=12)
        
        # Título del hito
        ax.text(x_pos, 0.65, titulo, ha='center', va='center',
                fontweight='bold', fontsize=10, 
                bbox=dict(boxstyle="round,pad=0.3", facecolor="lightblue"),
                transform=ax.transAxes)
        
        # Descripción
        ax.text(x_pos, 0.35, descripcion, ha='center', va='center',
                fontsize=8, transform=ax.transAxes)
        
        # Línea conectora
        ax.plot([x_pos, x_pos], [0.5, 0.6], 'b--', alpha=0.5)
    
    # Métricas finales
    ax.text(0.5, 0.15, 'RESULTADOS FINALES: 8 productos analizados • 150-500x mejora rendimiento • Portfolio completo',
            ha='center', va='center', fontsize=12, fontweight='bold',
            bbox=dict(boxstyle="round,pad=0.5", facecolor="lightgreen", alpha=0.8),
            transform=ax.transAxes)
    
    ax.set_xlim(0, 1)
    ax.set_ylim(0, 1)
    ax.axis('off')
    
    plt.tight_layout()
    plt.savefig('cronologia_proyecto_hadoop.png', dpi=300, bbox_inches='tight')
    print("✅ Cronología 'cronologia_proyecto_hadoop.png' generada")
    
    return fig

def main():
    """Genera todas las capturas explicativas"""
    print("🎨 GENERANDO CAPTURAS EXPLICATIVAS DEL PROCESO HADOOP")
    print("=" * 60)
    
    crear_diagrama_arquitectura()
    crear_diagrama_proceso_mapreduce() 
    crear_cronologia_proyecto()
    
    print("\n✅ ¡CAPTURAS EXPLICATIVAS GENERADAS!")
    print("📁 Archivos creados:")
    print("   • arquitectura_cluster_hadoop.png")
    print("   • proceso_mapreduce_hadoop.png")
    print("   • cronologia_proyecto_hadoop.png")

if __name__ == "__main__":
    main()
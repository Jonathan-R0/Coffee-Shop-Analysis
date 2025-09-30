#!/bin/bash

# Script para generar dinámicamente docker-compose.yaml con nodos de filtro configurables
# Sigue el patrón: .sh recibe parámetros y llama al .py

# Validar argumentos
if [ $# -lt 4 ] || [ $# -gt 7 ]; then
    echo "Uso: $0 <nombre_archivo> <cant_year> <cant_hour> <cant_amount> [include_report_generator] [groupby_top_customers] [topk_intermediate]"
    echo ""
    echo "Ejemplos:"
    echo "  $0 docker-compose.yaml 2 1 1                    # 2 nodos year, 1 hour, 1 amount + Q4 básico"
    echo "  $0 docker-compose.yaml 3 3 3 true               # 3 nodos year, 3 hour, 3 amount + Q4 básico"
    echo "  $0 docker-compose.yaml 3 3 3 true 3 2           # Configuración completa (como actual)"
    echo "  $0 docker-compose.yaml 1 2 0 false 5 3          # Sin report_generator, 5 groupby Q4, 3 topk"
    echo ""
    echo "Parámetros:"
    echo "  - include_report_generator: true/false (default: true)"
    echo "  - groupby_top_customers: cantidad de nodos GroupBy para Query 4 (default: 3)"
    echo "  - topk_intermediate: cantidad de nodos TopK intermediate (default: 2)"
    exit 1
fi

# Obtener parámetros
NOMBRE_ARCHIVO=$1
CANT_YEAR=$2
CANT_HOUR=$3
CANT_AMOUNT=$4
INCLUDE_REPORT_GENERATOR=${5:-true}  # Por defecto true
GROUPBY_TOP_CUSTOMERS=${6:-3}        # Por defecto 3
TOPK_INTERMEDIATE=${7:-2}            # Por defecto 2

# Validar que sean números
if ! [[ "$CANT_YEAR" =~ ^[0-9]+$ ]] || ! [[ "$CANT_HOUR" =~ ^[0-9]+$ ]] || ! [[ "$CANT_AMOUNT" =~ ^[0-9]+$ ]] || ! [[ "$GROUPBY_TOP_CUSTOMERS" =~ ^[0-9]+$ ]] || ! [[ "$TOPK_INTERMEDIATE" =~ ^[0-9]+$ ]]; then
    echo "❌ Error: Las cantidades deben ser números enteros positivos o cero"
    exit 1
fi

# Mostrar información
echo "📁 Nombre del archivo de salida: $NOMBRE_ARCHIVO"
echo "📊 Cantidad de nodos year: $CANT_YEAR"
echo "📊 Cantidad de nodos hour: $CANT_HOUR"
echo "📊 Cantidad de nodos amount: $CANT_AMOUNT"
echo "📈 Report generator: $INCLUDE_REPORT_GENERATOR"
echo "🔥 Query 4 - GroupBy top customers: $GROUPBY_TOP_CUSTOMERS"
echo "🔥 Query 4 - TopK intermediate: $TOPK_INTERMEDIATE"
echo "🔥 Query 4 - TopK final: 1 (siempre)"
echo ""

# Llamar al script Python con los parámetros
python3 configure_nodes.py "$NOMBRE_ARCHIVO" "$CANT_YEAR" "$CANT_HOUR" "$CANT_AMOUNT" "$INCLUDE_REPORT_GENERATOR" "$GROUPBY_TOP_CUSTOMERS" "$TOPK_INTERMEDIATE"

# Verificar si el script Python se ejecutó correctamente
if [ $? -eq 0 ]; then
    echo ""
    echo "🚀 Para ejecutar los servicios, use:"
    echo "   docker-compose -f $NOMBRE_ARCHIVO up -d --build"
else
    echo "❌ Error al generar el archivo docker-compose"
    exit 1
fi



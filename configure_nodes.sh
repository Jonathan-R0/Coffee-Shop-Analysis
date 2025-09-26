#!/bin/bash

# Script para generar dinámicamente docker-compose.yaml con nodos de filtro configurables
# Sigue el patrón: .sh recibe parámetros y llama al .py

# Validar argumentos
if [ $# -lt 4 ] || [ $# -gt 5 ]; then
    echo "Uso: $0 <nombre_archivo> <cant_year> <cant_hour> <cant_amount> [include_report_generator]"
    echo ""
    echo "Ejemplos:"
    echo "  $0 docker-compose.yaml 2 1 1       # 2 nodos year, 1 hour, 1 amount + report_generator"
    echo "  $0 docker-compose.yaml 0 0 3       # Solo 3 nodos amount + report_generator"
    echo "  $0 docker-compose.yaml 1 2 0 true  # 1 nodo year, 2 hour + report_generator"
    echo "  $0 docker-compose.yaml 1 2 0 false # 1 nodo year, 2 hour SIN report_generator"
    exit 1
fi

# Obtener parámetros
NOMBRE_ARCHIVO=$1
CANT_YEAR=$2
CANT_HOUR=$3
CANT_AMOUNT=$4
INCLUDE_REPORT_GENERATOR=${5:-true}  # Por defecto true

# Validar que sean números
if ! [[ "$CANT_YEAR" =~ ^[0-9]+$ ]] || ! [[ "$CANT_HOUR" =~ ^[0-9]+$ ]] || ! [[ "$CANT_AMOUNT" =~ ^[0-9]+$ ]]; then
    echo "❌ Error: Las cantidades deben ser números enteros positivos o cero"
    exit 1
fi

# Mostrar información
echo "📁 Nombre del archivo de salida: $NOMBRE_ARCHIVO"
echo "📊 Cantidad de nodos year: $CANT_YEAR"
echo "📊 Cantidad de nodos hour: $CANT_HOUR"
echo "📊 Cantidad de nodos amount: $CANT_AMOUNT"
echo "📈 Report generator: $INCLUDE_REPORT_GENERATOR"
echo ""

# Llamar al script Python con los parámetros
python3 configure_nodes.py "$NOMBRE_ARCHIVO" "$CANT_YEAR" "$CANT_HOUR" "$CANT_AMOUNT" "$INCLUDE_REPORT_GENERATOR"

# Verificar si el script Python se ejecutó correctamente
if [ $? -eq 0 ]; then
    echo ""
    echo "🚀 Para ejecutar los servicios, use:"
    echo "   docker-compose -f $NOMBRE_ARCHIVO up -d --build"
else
    echo "❌ Error al generar el archivo docker-compose"
    exit 1
fi



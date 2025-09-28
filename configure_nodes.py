#!/usr/bin/env python3
import sys

def base_services_setup(f):
    """Escribe los servicios base del docker-compose."""
    f.write(
        "services:\n"
        "  rabbitmq:\n"
        "    build:\n"
        "      context: ./rabbitmq\n"
        "      dockerfile: Dockerfile\n"
        "    ports:\n"
        "      - \"5672:5672\"\n"
        "      - \"15672:15672\"\n"
        "    healthcheck:\n"
        "      test: [\"CMD\", \"rabbitmq-diagnostics\", \"ping\"]\n"
        "      interval: 10s\n"
        "      timeout: 5s\n"
        "      retries: 10\n\n"
        
        "  client:\n"
        "    build:\n"
        "      context: .\n"
        "      dockerfile: ./client/Dockerfile\n"
        "    restart: on-failure\n"
        "    environment:\n"
        "      - PYTHONUNBUFFERED=1\n"
        "      - RABBITMQ_HOST=rabbitmq\n"
        "    depends_on:\n"
        "      gateway:\n"
        "        condition: service_started\n"
        "    volumes:\n"
        "      - ./client/config.ini:/config.ini\n"
        "      - ./data/transaction_items:/data/transaction_items\n"
        "      - ./data/users:/data/users\n"
        "      - ./data/stores:/data/stores\n"
        "      - ./data/menu_items:/data/menu_items\n"
        "      - ./data/payment_methods:/data/payment_methods\n"
        "      - ./data/vouchers:/data/vouchers\n\n"
        "      - ./data/transactions:/data/transactions\n\n"
        "      - ./data/transactions_test:/data/transactions_test\n\n"
        
        "  gateway:\n"
        "    build:\n"
        "      context: .\n"
        "      dockerfile: ./server/gateway/Dockerfile\n"
        "    container_name: gateway\n"
        "    restart: on-failure\n"
        "    depends_on:\n"
        "      rabbitmq:\n"
        "        condition: service_healthy\n"
        "    environment:\n"
        "      - PYTHONUNBUFFERED=1\n"
        "      - RABBITMQ_HOST=rabbitmq\n"
        "      - OUTPUT_QUEUE=raw_data\n"
        "    volumes:\n"
        "      - ./server/config.ini:/app/config.ini\n\n"
    )

def filter_service_setup(f, filter_type, instance_id, input_queue, output_queue=None, total_count=None):
    """Escribe un servicio de filtro específico."""
    service_name = f"filter_{filter_type}_{instance_id}"
    
    f.write(
        f"  {service_name}:\n"
        f"    build:\n"
        f"      context: .\n"
        f"      dockerfile: ./server/filter/Dockerfile\n"
        f"    container_name: {service_name}\n"
        f"    restart: on-failure\n"
        f"    depends_on:\n"
        f"      rabbitmq:\n"
        f"        condition: service_healthy\n"
        f"    environment:\n"
        f"      - PYTHONUNBUFFERED=1\n"
        f"      - RABBITMQ_HOST=rabbitmq\n"
        f"      - FILTER_MODE={filter_type}\n"
        f"      - INPUT_QUEUE={input_queue}\n"
    )
    
    # Agregar OUTPUT_Q1 solo si se especifica
    if output_queue:
        f.write(f"      - OUTPUT_Q1={output_queue}\n")
    
    # Agregar variable de entorno con el total de este tipo de filtro
    if total_count is not None:
        f.write(f"      - TOTAL_{filter_type.upper()}_FILTERS={total_count}\n")
    
    # Configuraciones específicas por tipo de filtro
    if filter_type == 'year':
        f.write("      - FILTER_YEARS=2024,2025\n")
    elif filter_type == 'hour':
        f.write("      - FILTER_HOURS=06:00-22:59\n")
    elif filter_type == 'amount':
        f.write("      - MIN_AMOUNT=75\n")
    
    f.write(
        f"    volumes:\n"
        f"      - ./server/config.ini:/app/config.ini\n\n"
    )


def report_generator_service_setup(f, input_queue):
    """Escribe el servicio de report_generator."""
    f.write(
        f"  report_generator:\n"
        f"    build:\n"
        f"      context: .\n"
        f"      dockerfile: ./server/report_generator/Dockerfile\n"
        f"    container_name: report_generator\n"
        f"    restart: on-failure\n"
        f"    depends_on:\n"
        f"      rabbitmq:\n"
        f"        condition: service_healthy\n"
        f"    environment:\n"
        f"      - PYTHONUNBUFFERED=1\n"
        f"      - RABBITMQ_HOST=rabbitmq\n"
        f"      - INPUT_QUEUE={input_queue}\n"
        f"    volumes:\n"
        f"      - ./server/config.ini:/app/config.ini\n"
        f"      - ./reports:/app/reports\n\n"
    )

def determine_queue_configuration(year_count, hour_count, amount_count, report_generator=True):
    """Determina la configuración de colas basándose en los filtros activos."""
    config = {}
    
    # Orden de procesamiento: year -> hour -> amount -> report_generator
    current_input = "raw_data"
    
    if year_count > 0:
        output = "year_filtered" if (hour_count > 0 or amount_count > 0) else ("final_data" if report_generator else None)
        config['year'] = {'input': current_input, 'output': output}
        current_input = "year_filtered"
    
    if hour_count > 0:
        input_queue = current_input if year_count > 0 else "raw_data"
        output = "hour_filtered" if amount_count > 0 else ("final_data" if report_generator else None)
        config['hour'] = {'input': input_queue, 'output': output}
        current_input = "hour_filtered"
    
    if amount_count > 0:
        input_queue = current_input if (year_count > 0 or hour_count > 0) else "raw_data"
        config['amount'] = {'input': input_queue, 'output': "final_data" if report_generator else None}
        current_input = "final_data"
    
    # Si no hay filtros pero hay report_generator, toma directamente de raw_data
    if report_generator and year_count == 0 and hour_count == 0 and amount_count == 0:
        current_input = "raw_data"
    
    if report_generator:
        config['report_generator'] = {'input': current_input, 'output': None}
    
    return config

def setup_docker_compose(filename, year_count, hour_count, amount_count, include_report_generator=True):
    """Genera el archivo docker-compose.yaml completo."""
    with open(filename, 'w') as f:
        # Servicios base
        base_services_setup(f)
        
        # Determinar configuración de colas
        queue_config = determine_queue_configuration(year_count, hour_count, amount_count, include_report_generator)
        
        # Generar servicios de filtro year
        if year_count > 0:
            input_queue = queue_config['year']['input']
            output_queue = queue_config['year']['output']
            for i in range(1, year_count + 1):
                filter_service_setup(f, 'year', i, input_queue, output_queue, year_count)
        
        # Generar servicios de filtro hour
        if hour_count > 0:
            input_queue = queue_config['hour']['input']
            output_queue = queue_config['hour']['output']
            for i in range(1, hour_count + 1):
                filter_service_setup(f, 'hour', i, input_queue, output_queue, hour_count)
        
        # Generar servicios de filtro amount
        if amount_count > 0:
            input_queue = queue_config['amount']['input']
            output_queue = queue_config['amount']['output']
            for i in range(1, amount_count + 1):
                filter_service_setup(f, 'amount', i, input_queue, output_queue, amount_count)
        
        # Generar servicio de report_generator
        if include_report_generator:
            input_queue = queue_config['report_generator']['input']
            report_generator_service_setup(f, input_queue)

def main():
    if len(sys.argv) < 5 or len(sys.argv) > 6:
        print("Uso: python configure_nodes.py <nombre_archivo> <cant_year> <cant_hour> <cant_amount> [include_report_generator]")
        print("Ejemplos:")
        print("  python configure_nodes.py docker-compose.yaml 1 1 1        # Con report_generator (por defecto)")
        print("  python configure_nodes.py docker-compose.yaml 1 1 1 true   # Con report_generator")
        print("  python configure_nodes.py docker-compose.yaml 1 1 1 false  # Sin report_generator")
        sys.exit(1)

    nombre_archivo = sys.argv[1]
    cant_year = int(sys.argv[2])
    cant_hour = int(sys.argv[3])
    cant_amount = int(sys.argv[4])
    
    # Por defecto incluir report_generator, a menos que se especifique false
    include_report_generator = True
    if len(sys.argv) == 6:
        include_report_generator = sys.argv[5].lower() not in ['false', '0', 'no']

    setup_docker_compose(nombre_archivo, cant_year, cant_hour, cant_amount, include_report_generator)
    print(f"✅ Archivo '{nombre_archivo}' generado exitosamente!")
    print(f"📊 Filtros year: {cant_year}, hour: {cant_hour}, amount: {cant_amount}")
    if include_report_generator:
        print("📈 Report generator: incluido")
    else:
        print("📈 Report generator: no incluido")

if __name__ == "__main__":
    main()

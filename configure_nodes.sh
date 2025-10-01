#!/bin/bash

if [ $# -ne 11 ]; then
    echo "Usage: $0 <filename> <year_transactions> <year_transaction_items> <hour> <amount> <groupby_2024> <groupby_2025> <aggregator_2024> <aggregator_2025> <groupby_top_customers> <top_consumers_aggregator>"
    echo ""
    echo "Examples:"
    echo "  $0 docker-compose.yaml 2 2 3 3 2 1 1 1 2 2"
    echo "  $0 test-compose.yaml 1 1 1 1 1 1 1 1 1 1"
    exit 1
fi

FILENAME=$1
YEAR_TRANSACTIONS=$2
YEAR_TRANSACTION_ITEMS=$3
HOUR=$4
AMOUNT=$5
GROUPBY_2024=$6
GROUPBY_2025=$7
AGGREGATOR_2024=$8
AGGREGATOR_2025=$9
GROUPBY_TOP_CUSTOMERS=${10}
TOP_CONSUMERS_AGGREGATOR=${11}

for arg in "$YEAR_TRANSACTIONS" "$YEAR_TRANSACTION_ITEMS" "$HOUR" "$AMOUNT" "$GROUPBY_2024" "$GROUPBY_2025" "$AGGREGATOR_2024" "$AGGREGATOR_2025" "$GROUPBY_TOP_CUSTOMERS" "$TOP_CONSUMERS_AGGREGATOR"; do
    if ! [[ "$arg" =~ ^[0-9]+$ ]]; then
        echo "Error: All counts must be non-negative integers"
        exit 1
    fi
done

python3 configure_nodes.py "$FILENAME" "$YEAR_TRANSACTIONS" "$YEAR_TRANSACTION_ITEMS" "$HOUR" "$AMOUNT" "$GROUPBY_2024" "$GROUPBY_2025" "$AGGREGATOR_2024" "$AGGREGATOR_2025" "$GROUPBY_TOP_CUSTOMERS" "$TOP_CONSUMERS_AGGREGATOR"

if [ $? -eq 0 ]; then
    echo "To run the services, use:"
    echo "  docker-compose -f $FILENAME up -d --build"
else
    echo "Error generating docker-compose file"
    exit 1
fi
"""
Módulo de filtros para el sistema Coffee Shop Analysis.

Este módulo implementa el patrón Strategy para filtrar transacciones
basándose en diferentes criterios como año, hora y cantidad mínima.
"""

from base_strategy import FilterStrategy
from year_filter_strategy import YearFilterStrategy
from hour_filter_strategy import HourFilterStrategy
from amount_filter_strategy import AmountFilterStrategy
from filter_factory import FilterStrategyFactory
from main import FilterNode

__all__ = [
    'FilterStrategy',
    'YearFilterStrategy', 
    'HourFilterStrategy',
    'AmountFilterStrategy',
    'FilterStrategyFactory',
    'FilterNode'
]

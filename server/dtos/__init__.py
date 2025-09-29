"""
Módulo de DTOs para el manejo de archivos CSV en el sistema de análisis de Coffee Shop.

Este módulo proporciona una arquitectura modular y extensible para manejar diferentes
tipos de archivos CSV a través del patrón DTO (Data Transfer Object) y Factory.

Estructura:
- enums: Define los tipos de archivo y batch soportados
- base_dto: Clase abstracta base para todos los DTOs
- *_dto: Implementaciones específicas para cada tipo de archivo
- dto_factory: Factory para crear DTOs de manera consistente

Uso básico:
    from server.dtos import DTOFactory, FileType, BatchType
    
    # Crear un DTO específico
    transaction_dto = DTOFactory.create_dto(
        FileType.TRANSACTIONS, 
        data, 
        BatchType.DATA
    )

Compatibilidad:
    Este módulo mantiene compatibilidad completa con el código existente.
    Las clases pueden importarse individualmente si es necesario.
"""

# Enums principales
from .enums import FileType, BatchType

# Clase base
from .base_dto import BaseDTO

# DTOs específicos
from .transaction_dto import TransactionBatchDTO
from .user_dto import UserBatchDTO
from .store_dto import StoreBatchDTO
from .menu_item_dto import MenuItemBatchDTO
from .transaction_item_dto import TransactionItemBatchDTO

# Factory
from .dto_factory import DTOFactory

# Para mantener compatibilidad con código existente
__all__ = [
    # Enums
    'FileType',
    'BatchType',
    
    # Clase base
    'BaseDTO',
    
    # DTOs específicos
    'TransactionBatchDTO',
    'UserBatchDTO', 
    'StoreBatchDTO',
    'MenuItemBatchDTO',
    'TransactionItemBatchDTO',
    
    # Factory
    'DTOFactory'
]

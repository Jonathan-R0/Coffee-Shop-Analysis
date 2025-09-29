from .base_dto import BaseDTO
from .enums import FileType, BatchType
from .transaction_dto import TransactionBatchDTO
from .user_dto import UserBatchDTO
from .store_dto import StoreBatchDTO
from .menu_item_dto import MenuItemBatchDTO
from .transaction_item_dto import TransactionItemBatchDTO


class DTOFactory:
    """
    Factory para crear DTOs según el tipo de archivo.
    Implementa el patrón Factory Method para crear instancias específicas de DTOs.
    """
    
    _dto_mapping = {
        FileType.TRANSACTIONS: TransactionBatchDTO,
        FileType.USERS: UserBatchDTO,
        FileType.STORES: StoreBatchDTO,
        FileType.MENU_ITEMS: MenuItemBatchDTO,
        FileType.TRANSACTION_ITEMS: TransactionItemBatchDTO,
    }
    
    @classmethod
    def create_dto(cls, file_type: FileType, data, batch_type=BatchType.DATA) -> BaseDTO:
        """
        Crea un DTO específico según el tipo de archivo.
        
        Args:
            file_type: Tipo de archivo (enum FileType)
            data: Datos a encapsular
            batch_type: Tipo de batch (enum BatchType)
            
        Returns:
            BaseDTO: Instancia del DTO específico
            
        Raises:
            ValueError: Si el tipo de archivo no está soportado
        """
        dto_class = cls._dto_mapping.get(file_type)
        if not dto_class:
            raise ValueError(f"Tipo de archivo no soportado: {file_type}")
        
        return dto_class(data, batch_type)
    
    @classmethod
    def get_supported_file_types(cls) -> list:
        """
        Retorna la lista de tipos de archivos soportados.
        
        Returns:
            list: Lista de FileType enums soportados
        """
        return list(cls._dto_mapping.keys())
    
    @classmethod
    def register_dto(cls, file_type: FileType, dto_class):
        """
        Registra un nuevo tipo de DTO en el factory.
        Útil para extensibilidad futura.
        
        Args:
            file_type: Tipo de archivo
            dto_class: Clase DTO que debe heredar de BaseDTO
        """
        if not issubclass(dto_class, BaseDTO):
            raise ValueError("La clase DTO debe heredar de BaseDTO")
        
        cls._dto_mapping[file_type] = dto_class

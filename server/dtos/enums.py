from enum import Enum


class FileType(Enum):
    """Enumeración para los tipos de archivos soportados"""
    TRANSACTIONS = "transactions"
    USERS = "users"
    STORES = "stores"
    MENU_ITEMS = "menu_items"
    PAYMENT_METHODS = "payment_methods"
    VOUCHERS = "vouchers"
    TRANSACTION_ITEMS = "transaction_items"


class BatchType(Enum):
    """Enumeración para los tipos de batch"""
    DATA = "DATA"
    CONTROL = "CONTROL"
    RAW_CSV = "RAW_CSV"
    EOF = "EOF"

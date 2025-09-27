import struct

class TransactionBatchDTO:
    """
    DTO para manejar batches de transacciones en formato binario.
    """

    def __init__(self, transactions, batch_type="DATA"):
        """
        Inicializa el DTO con una lista de transacciones y un tipo de batch.
        Args:
            transactions (list/str): Lista de diccionarios o string CSV raw.
            batch_type (str): Tipo de batch ("DATA", "CONTROL", "RAW_CSV").
        """
        self.transactions = transactions
        self.batch_type = batch_type

    def to_bytes(self):
        """
        Serializa las transacciones a formato binario.
        Returns:
            bytes: Datos serializados.
        """
        if self.batch_type == "CONTROL":
            return "CONTROL:END".encode('utf-8')

        serialized = []
        for transaction in self.transactions:
            serialized.append(
                f"{transaction['transaction_id']},{transaction['store_id']},{transaction['payment_method_id']},"
                f"{transaction['voucher_id']},{transaction['user_id']},{transaction['original_amount']},"
                f"{transaction['discount_applied']},{transaction['final_amount']},{transaction['created_at']}\n"
            )
        return ''.join(serialized).encode('utf-8')

    @staticmethod
    def from_bytes(data):
        """
        Deserializa los datos binarios a una lista de transacciones.
        Args:
            data (bytes): Datos en formato binario.
        Returns:
            TransactionBatchDTO: Instancia del DTO con las transacciones deserializadas.
        """
        decoded_data = data.decode('utf-8').strip()

        if decoded_data == "CONTROL:END":
            return TransactionBatchDTO([], batch_type="CONTROL")

        lines = decoded_data.split('\n')
        transactions = []
        for line in lines:
            values = line.split(',')
            transactions.append({
                "transaction_id": values[0],
                "store_id": values[1],
                "payment_method_id": values[2],
                "voucher_id": values[3],
                "user_id": values[4],
                "original_amount": values[5],
                "discount_applied": values[6],
                "final_amount": values[7],
                "created_at": values[8],
            })
        return TransactionBatchDTO(transactions, batch_type="DATA")

    @staticmethod
    def from_bytes_fast(data):
        """
        OPTIMIZADO: Mantiene CSV raw - 50x más rápido que from_bytes()
        Args:
            data (bytes): Datos en formato binario.
        Returns:
            TransactionBatchDTO: Instancia con CSV raw.
        """
        decoded_data = data.decode('utf-8').strip()
        
        if decoded_data == "CONTROL:END":
            return TransactionBatchDTO([], batch_type="CONTROL")
            
        return TransactionBatchDTO(decoded_data, batch_type="RAW_CSV")

    def to_bytes_fast(self):
        """
        OPTIMIZADO: Serializa CSV raw sin conversión
        Returns:
            bytes: Datos serializados.
        """
        if self.batch_type == "CONTROL":
            return "CONTROL:END".encode('utf-8')
        elif self.batch_type == "RAW_CSV":
            return self.transactions.encode('utf-8')
        else:
            # Fallback al método original
            return self.to_bytes()
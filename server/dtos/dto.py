import struct

class TransactionBatchDTO:
    """
    DTO para manejar batches de transacciones en formato binario.
    """

    def __init__(self, transactions):
        """
        Inicializa el DTO con una lista de transacciones.
        Args:
            transactions (list): Lista de diccionarios que representan las transacciones.
        """
        self.transactions = transactions

    def to_bytes(self):
        """
        Serializa las transacciones a formato binario.
        Returns:
            bytes: Datos serializados.
        """
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
        lines = data.decode('utf-8').strip().split('\n')
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
        return TransactionBatchDTO(transactions)
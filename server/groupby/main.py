import logging
import os
from collections import defaultdict
from rabbitmq.middleware import MessageMiddlewareQueue
from server.dtos.dto import TransactionBatchDTO

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class GroupByNode:
    def __init__(self):
        self.rabbitmq_host = os.getenv('RABBITMQ_HOST', 'localhost')
        
        self.input_queue = os.getenv('INPUT_QUEUE', 'filter.groupby')
        self.output_queue_most_sold = os.getenv('OUTPUT_QUEUE_MOST_SOLD', 'groupby.aggregator.most_sold')
        self.output_queue_most_profitable = os.getenv('OUTPUT_QUEUE_MOST_PROFITABLE', 'groupby.aggregator.most_profitable')
        self.output_queue_semester = os.getenv('OUTPUT_QUEUE_SEMESTER', 'groupby.join.semester')
        
        self.query_type = os.getenv('QUERY_TYPE', None)
        if self.query_type not in ['2', '3']:
            raise ValueError("QUERY_TYPE must be '2' or '3'")
        
        self.input_middleware = MessageMiddlewareQueue(
            host=self.rabbitmq_host, 
            queue_name=self.input_queue
        )
        
        self.output_middleware_most_sold = MessageMiddlewareQueue(
            host=self.rabbitmq_host, 
            queue_name=self.output_queue_most_sold
        )
        
        self.output_middleware_most_profitable = MessageMiddlewareQueue(
            host=self.rabbitmq_host, 
            queue_name=self.output_queue_most_profitable
        )
        
        self.output_middleware_semester = MessageMiddlewareQueue(
            host=self.rabbitmq_host, 
            queue_name=self.output_queue_semester
        )
        
        logger.info(f"GroupByNode initialized:")
        logger.info(f"  Tipo de query: {self.query_type}")
        logger.info(f"  Queue entrada: {self.input_queue}")
        logger.info(f"  Queue salida: {self.output_queue_most_sold}, {self.output_queue_most_profitable}, {self.output_queue_semester}")

    @staticmethod
    def get_month_from_csv_line(line):
        """Extract month from CSV line - OPTIMIZED version"""
        # CSV format: transaction_id,store_id,payment_method_id,voucher_id,user_id,original_amount,discount_applied,final_amount,created_at
        fields = line.split(',')
        if len(fields) >= 9:
            date_str = fields[8]  # created_at is the 9th field (index 8)
            return int(date_str[5:7])  # Extract month from YYYY-MM-DD format
        return None

    @staticmethod
    def get_year_from_csv_line(line):
        """Extract year from CSV line - OPTIMIZED version"""
        fields = line.split(',')
        if len(fields) >= 9:
            date_str = fields[8]  # created_at is the 9th field (index 8)
            return int(date_str[0:4])  # Extract year from YYYY-MM-DD format
        return None

    @staticmethod
    def get_semester_from_csv_line(line):
        """Extract semester from CSV line - OPTIMIZED version"""
        month = GroupByNode.get_month_from_csv_line(line)
        if month:
            return 1 if month <= 6 else 2
        return None

    @staticmethod
    def get_store_id_from_csv_line(line):
        """Extract store_id from CSV line - OPTIMIZED version"""
        fields = line.split(',')
        if len(fields) >= 2:
            return fields[1]  # store_id is the 2nd field (index 1)
        return None

    def group_by_month_item_csv(self, csv_data):
        """
        OPTIMIZED: Groups CSV data by month and item_id without creating Python objects.
        For Query 2: groups by month for aggregation.
        """
        grouped = defaultdict(list)
        lines = csv_data.strip().split('\n')
        
        for line in lines:
            if not line.strip():
                continue
            month = self.get_month_from_csv_line(line)
            if month:
                # For transactions, we group by month only since there's no item_id
                # The aggregator will need to handle the lack of item_id
                grouped[month].append(line)
        
        return grouped

    def group_by_semester_year_store_csv(self, csv_data):
        """
        OPTIMIZED: Groups CSV data by semester, year, and store without creating Python objects.
        For Query 3: groups by semester, year, and store.
        """
        grouped = defaultdict(list)
        lines = csv_data.strip().split('\n')
        
        for line in lines:
            if not line.strip():
                continue
            semester = self.get_semester_from_csv_line(line)
            year = self.get_year_from_csv_line(line)
            store_id = self.get_store_id_from_csv_line(line)
            
            if semester and year and store_id:
                key = (semester, year, store_id)
                grouped[key].append(line)
        
        return grouped

    def process_message(self, message: bytes):
        """
        OPTIMIZED: Processes CSV directly without full deserialization.
        """
        try:
            # FAST PATH: Use optimized CSV processing
            dto = TransactionBatchDTO.from_bytes_fast(message)

            if dto.batch_type == "CONTROL":
                logger.info("CONTROL signal received - propagating to next nodes")
                self.output_middleware_most_sold.send(message)
                self.output_middleware_most_profitable.send(message)
                self.output_middleware_semester.send(message)
                return

            # Count input lines
            input_lines = len(dto.transactions.split('\n')) if dto.transactions else 0
            
            if not dto.transactions.strip():
                logger.info(f"Empty batch received - {input_lines} lines")
                return

            if self.query_type == '2':
                # Query 2: Group by month for aggregation
                grouped = self.group_by_month_item_csv(dto.transactions)
                total_output_lines = 0
                
                for month, lines in grouped.items():
                    if not lines:
                        continue
                    
                    # Create CSV data for this month group
                    csv_data = '\n'.join(lines)
                    grouped_dto = TransactionBatchDTO(csv_data, batch_type="RAW_CSV")
                    serialized_data = grouped_dto.to_bytes_fast()
                    
                    # Send to both aggregators
                    self.output_middleware_most_sold.send(serialized_data)
                    self.output_middleware_most_profitable.send(serialized_data)
                    
                    total_output_lines += len(lines)
                    logger.info(f"Month {month}: {len(lines)} lines sent to aggregators")
                
                logger.info(f"Query 2 processed OPTIMIZED: {input_lines} lines → {total_output_lines} lines grouped")
                
            elif self.query_type == '3':
                # Query 3: Group by semester, year, store
                grouped = self.group_by_semester_year_store_csv(dto.transactions)
                total_output_lines = 0
                
                for (semester, year, store_id), lines in grouped.items():
                    if not lines:
                        continue
                    
                    # Create CSV data for this group
                    csv_data = '\n'.join(lines)
                    grouped_dto = TransactionBatchDTO(csv_data, batch_type="RAW_CSV")
                    serialized_data = grouped_dto.to_bytes_fast()
                    
                    # Send to join node
                    self.output_middleware_semester.send(serialized_data)
                    
                    total_output_lines += len(lines)
                    logger.info(f"Semester {semester}, Year {year}, Store {store_id}: {len(lines)} lines sent to join")
                
                logger.info(f"Query 3 processed OPTIMIZED: {input_lines} lines → {total_output_lines} lines grouped")

        except Exception as e:
            logger.error(f"Error processing message: {e}")

    def on_message_callback(self, ch, method, properties, body):
        """
        Callback for RabbitMQ when a message arrives.
        """
        try:
            self.process_message(body)
        except Exception as e:
            logger.error(f"Error in callback: {e}")

    def start(self):
        try:
            self.input_middleware.start_consuming(self.on_message_callback)
        except KeyboardInterrupt:
            logger.info("GroupByNode stopped manually")
        except Exception as e:
            logger.error(f"Error during consumption: {e}")
            raise
        finally:
            self._cleanup()

    def _cleanup(self):
        try:
            if self.input_middleware:
                self.input_middleware.close()
                logger.info("Input connection closed")
                
            if self.output_middleware_most_sold:
                self.output_middleware_most_sold.close()
                logger.info("Most sold output connection closed")
                
            if self.output_middleware_most_profitable:
                self.output_middleware_most_profitable.close()
                logger.info("Most profitable output connection closed")
                
            if self.output_middleware_semester:
                self.output_middleware_semester.close()
                logger.info("Semester output connection closed")
                
        except Exception as e:
            logger.error(f"Error during cleanup: {e}")

if __name__ == "__main__":
    node = GroupByNode()
    node.start()
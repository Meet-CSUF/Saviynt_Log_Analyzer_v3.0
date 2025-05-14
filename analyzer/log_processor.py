import json
import logging
from datetime import datetime

# Configure logging
logging.basicConfig(
    filename='log_analyzer.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class LogProcessor:
    def parse_log_line(self, log_line: str) -> dict:
        """Parse a JSON log line into a structured dictionary."""
        try:
            log_entry = json.loads(log_line.strip())
            timestamp = log_entry.get('logtime', '')
            level = log_entry.get('level', 'UNKNOWN')
            class_field = log_entry.get('class', None)
            log_message = log_entry.get('log', '')
            
            # Extract class and service
            if class_field and '.' in class_field:
                service, class_name = class_field.split('.', 1)
            else:
                class_name = 'Unknown'
                service = 'Unknown'
            
            # Validate timestamp
            if timestamp:
                try:
                    try:
                        datetime.strptime(timestamp, '%Y-%m-%d %H:%M:%S,%f')
                    except ValueError:
                        try:
                            datetime.strptime(timestamp, '%Y-%m-%d %H:%M:%S')
                        except ValueError:
                            datetime.strptime(timestamp, '%d/%b/%Y:%H:%M:%S %z')
                except ValueError:
                    timestamp = ''
            
            return {
                'logtime': timestamp,
                'level': level,
                'class': class_name,
                'service': service,
                'log': log_message
            }
        except json.JSONDecodeError:
            logger.warning(f"Invalid JSON log line: {log_line}")
            return None
        except Exception as e:
            logger.error(f"Error parsing log line: {str(e)}")
            return None
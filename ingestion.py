import json
import time
from datetime import datetime
import boto3
from kafka import KafkaConsumer
from botocore.exceptions import ClientError

class BronzeIngestion:
    def __init__(self, config_path="config.json"):
        with open(config_path, 'r') as f:
            self.config = json.load(f)
            
        self.consumer = KafkaConsumer(
            'clickstream_raw',
            bootstrap_servers=self.config['kafka_broker'],
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            auto_offset_reset='earliest',
            enable_auto_commit=True
        )
        
        self.s3 = boto3.client(
            's3',
            endpoint_url=self.config['minio_endpoint'],
            aws_access_key_id='admin',          
            aws_secret_access_key='password123' 
        )
        self.bucket = self.config['raw_bucket']

    def is_valid_event(self, event):
        required_keys = ["event_id", "session_id", "event_type", "event_time"]
        return all(key in event for key in required_keys)

    def write_to_minio(self, event, is_dlq=False):
            # Extract event_time from the payload for Event-Time Partitioning
            # Fall back to current UTC time if the field is missing or malformed (for the DLQ)
            try:
                event_dt = datetime.utcfromtimestamp(event['event_time'])
            except (KeyError, TypeError):
                event_dt = datetime.utcnow()
                
            # Partition raw data by date/hour using the event's actual timestamp
            partition_path = f"year={event_dt.year}/month={event_dt.month:02d}/day={event_dt.day:02d}/hour={event_dt.hour:02d}"
            
            prefix = "dlq" if is_dlq else "events"
            # Keep processing time in the filename to ensure uniqueness
            file_name = f"{prefix}/{partition_path}/event_{int(time.time() * 1000)}.json"
            
            max_retries = 3
            for attempt in range(max_retries):
                try:
                    self.s3.put_object(
                        Bucket=self.bucket,
                        Key=file_name,
                        Body=json.dumps(event)
                    )
                    print(f"[{'DLQ' if is_dlq else 'SUCCESS'}] Wrote to {file_name}")
                    break
                except ClientError as e:
                    if attempt == max_retries - 1:
                        print(f"[ERROR] Failed to write after retries: {e}")
                    else:
                        time.sleep(2 ** attempt)

    def consume_stream(self):
        print("Starting Bronze Ingestion Consumer...")
        for message in self.consumer:
            event = message.value
            
            if self.is_valid_event(event):
                self.write_to_minio(event, is_dlq=False)
            else:
                self.write_to_minio(event, is_dlq=True)

if __name__ == "__main__":
    ingestion = BronzeIngestion()
    ingestion.consume_stream()

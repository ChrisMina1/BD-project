import json
import time
import random
import uuid
from faker import Faker
from kafka import KafkaProducer
import threading

# Initialize Faker with a seed for reproducibility
fake = Faker()
Faker.seed(42) 

class ClickstreamGenerator:
    def __init__(self, config_path="config.json"):
        with open(config_path, 'r') as f:
            self.config = json.load(f)
        
        self.producer = KafkaProducer(
            bootstrap_servers=self.config['kafka_broker']
        )
        
        # Funnel events
        self.event_types = ["page_view", "search", "add_to_cart", "checkout", "purchase"]

    def generate_event(self, session_id, user_id, is_bad_event=False):
        """Generates a single clickstream event or a deliberate malformed event."""
        if is_bad_event:
            # Deliberate bad event to prove the DLQ works
            return {"broken_schema_field": "invalid_data", "timestamp": "bad_time"}

        return {
            "event_id": str(uuid.uuid4()),
            "session_id": session_id,
            "user_id": user_id,
            "event_type": random.choice(self.event_types),
            "event_time": time.time(),
            "platform": random.choice(["web", "mobile"]),
            "channel": random.choice(["organic", "paid_social", "email"]),
            "product_id": f"PROD-{random.randint(100, 999)}",
            "price": round(random.uniform(10.0, 150.0), 2),
            "search_filter": random.choice(["price_low_high", "top_rated", None]),
            "device": random.choice(["iOS", "Android", "Windows", "Mac"]),
            "anomaly_label": "none"
        }

    def send_event(self, event):
        """Manually encodes the dictionary to bytes before sending to Kafka to avoid deprecation warnings."""
        encoded_event = json.dumps(event).encode('utf-8')
        self.producer.send('clickstream_raw', encoded_event)

    def simulate_session(self):
        """Simulates a user session concurrently, with probability-based anomalies."""
        session_id = str(uuid.uuid4())
        user_id = f"USER-{random.randint(1000, 9999)}"
        
        # Increased probability to 30% to guarantee visibility in small samples
        is_abandoned = self.config['anomalies']['cart_abandonment'] and random.random() < 0.30
        
        for _ in range(random.randint(3, 10)):
            event = self.generate_event(session_id, user_id)
            
            # Anomaly: Cart Abandonment (Hidden Label)
            if is_abandoned and event["event_type"] == "add_to_cart":
                event["anomaly_label"] = "cart_abandonment"
                self.send_event(event)
                break # Terminate session immediately before purchase

            self.send_event(event)
            
            # Anomaly: Duplicate Events (5% chance to send the exact same event again)
            if random.random() < 0.05:
                self.send_event(event)

            time.sleep(random.uniform(0.1, 0.5))

        # Anomaly: Malformed Events for DLQ testing (Increased to 15% chance per session)
        if self.config['inject_bad_events'] and random.random() < 0.15:
            self.send_event(self.generate_event(session_id, user_id, is_bad_event=True))

    def run_concurrent_generator(self, active_sessions=5):
        """Runs the standard real-time concurrent generation."""
        threads = []
        for _ in range(active_sessions):
            t = threading.Thread(target=self.simulate_session)
            t.start()
            threads.append(t)
            
        for t in threads:
            t.join()
        self.producer.flush()

    def simulate_promo_surge(self):
        """Simulates a seasonal promo surge by blasting a high volume of sessions."""
        if self.config['anomalies']['promo_surge']:
            print("🚀 TRIGGERING PROMO SURGE: 50 concurrent sessions!")
            self.run_concurrent_generator(active_sessions=50)

if __name__ == "__main__":
    generator = ClickstreamGenerator()
    print("Starting Continuous Real-Time Data Generation (Press Ctrl+C to stop)...")
    
    loop_count = 0
    try:
        # Continuous production to simulate a real streaming source
        while True:
            loop_count += 1
            generator.run_concurrent_generator(active_sessions=random.randint(5, 15))
            print(f"Batch {loop_count} generated.")
            
            # Trigger a promo surge randomly roughly every 10 loops
            if random.random() < 0.10:
                generator.simulate_promo_surge()
                
            time.sleep(2) # Brief pause between normal traffic batches
            
    except KeyboardInterrupt:
        print("\nStreaming stopped safely.")
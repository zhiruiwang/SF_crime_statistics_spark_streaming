from kafka import KafkaConsumer
import time

# Kafka consumer to test data moved to topic
class ConsumerServer(KafkaConsumer):
    
    def __init__(self, topic, **kwargs):
        # Configure a Kafka Consumer
        super().__init__(**kwargs)
        self.topic = topic
        self.subscribe(topics = topic)
    
    def consume(self):
        try:
            while True:
                #Poll data for values
                for _, consumer_record in self.poll().items():
                    if consumer_record:
                        for record in consumer_record:
                            print(record.value)
                            time.sleep(0.2)
                    else:
                        print("No message received by consumer")
                        
                time.sleep(0.5)
        except:
            print("Closing consumer")
            self.close()
            
if __name__ == "__main__":
    consumer = ConsumerServer(topic="org.sf.police.calls",
                              bootstrap_servers = "localhost:9092", 
                              request_timeout_ms = 1000,
                              auto_offset_reset="earliest", 
                              max_poll_records=10)
    consumer.consume()
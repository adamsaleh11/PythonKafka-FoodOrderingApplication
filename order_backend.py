import json
import time

from kafka import KafkaProducer

ORDER_KAFKA_TOPIC = "order_details"
ORDER_LIMIT = 15

producer = KafkaProducer(boostrap_server= "localhost:29092")

print("Going to be generating order after 10 seconds")
print("Will generate one unique every 10 seconds")

for i in range(1, ORDER_LIMIT):
	data = {
        "order_id": 1, 
        "user_id": f"adam_{i}",
        "total_cost":i*2,
        "items": "burger, sandwich"
    }
producer.send(
    ORDER_KAFKA_TOPIC,
    json.dumps(data).encode("utf-8") #encoding data
)
print(f"Done sending...{i}")
time.sleep(10)

#producer is sending messages to Kafka pipeline, in the form of a dictionary called "data"

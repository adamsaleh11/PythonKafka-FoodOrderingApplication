import json
from kafka import KafkaConsumer

ORDER_CONFIRMED_KAFKA_TOPIC = "order_confirmed"

consumer = KafkaConsumer(
    ORDER_CONFIRMED_KAFKA_TOPIC,
    boostrap_servers = "localhost:29092"
)

email_sent_so_far = set()
print("Waiting for email confirmation")

while True:
    for message in consumer:
        consumed_message = json.loads(message.value.decode()),
        customer_email = consumed_message["customer_email"]

        print(f"{customer_email}, your order has been confirmed. It will be there shortly.")
        
        email_sent_so_far.add(customer_email)
        

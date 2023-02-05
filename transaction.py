import json

from kafka import KafkaConsumer
from kafka import KafkaProducer

ORDER_KAFKA_TOPIC = "order_details"
ORDER_CONFIRMED_KAFKA_TOPIC = "order_confirmed"

consumer = KafkaConsumer(
    ORDER_KAFKA_TOPIC,
    boostrap_servers="localhost:29092"
)

producer = KafkaProducer(
    boostrap_servers="localhost:29092"
)

print("Starting to listen")

#listening for message from producer forever
while True:
    for message in consumer:
        print("Ongoing transaction")
        consumed_message = message.value.decode ##payload of data that was sent from producer data pipeline
        #message needs to deocded because it was encoded as "utf-8"
        print(consumed_message)
        user_id = consumed_message["user_id"] #extracting the message sent by producer, by its different attributes
        total_cost = consumed_message["total_cost"]

        data = { #processing the data sent by the initial producer
            "customer_id": user_id,
            "customer_email": f"{user_id}@gmail.com",
            "total_cost": total_cost,
            "Message": "Your order has been confirmed"
        }
        #sending a confirmation back to the data pipeline as a producer, with an included attribute to the data dictionary that gives a simple "successful message"
        producer.send(
            ORDER_CONFIRMED_KAFKA_TOPIC,
            json.dumps(data).encode("utf-8")
        )
        
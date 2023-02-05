import json
from kafka import KafkaConsumer

ORDER_CONFIRMED_KAFKA_TOPIC = 'order_confirmed'

consumer = KafkaConsumer(
    ORDER_CONFIRMED_KAFKA_TOPIC,
    boostrap_servers="localhost:29092"
)

total_orders_count = 0
total_revenue = 0
averageordercost = 0

print("Waiting for order to be confirmed.")

while True:
    for message in consumer:
        print("We are making money!")
        consumed_message = json.loads(message.value.decode())

        total_cost = float(consumed_message["total_cost"])

        total_orders_count+=1
        total_revenue+=total_cost
        averageordercost = total_revenue/total_orders_count

        print(f"Orders so far today: {total_orders_count}")
        print(f"Total revenue today: {total_revenue}")
        print(f"Average order cost today: {averageordercost}")

        



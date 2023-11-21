import pika
import json
import os.path
import sys


def load_store_data():
    if os.path.getsize("store_data.json"):
        with open("store_data.json") as file:
            return json.load(file)
    return {}


if __name__ == '__main__':
    store_data = load_store_data()
    connection = pika.BlockingConnection(pika.ConnectionParameters("localhost"))
    channel = connection.channel()
    channel.queue_declare(queue="store_data_request")

    def request_callback(ch, method, properties, body):
        print("Received request!")
        store_product = body.decode()
        if type(store_product) != str:
            product_data_list = []
        else:
            product_data_list = [p for p in store_data["products"].items() if store_product in p[0]]

        ch.basic_publish(exchange="",
                         routing_key=properties.reply_to,
                         properties=pika.BasicProperties(correlation_id=properties.correlation_id),
                         body=json.dumps(product_data_list))
        ch.basic_ack(delivery_tag=method.delivery_tag)

    channel.basic_consume(queue="store_data_request",
                          on_message_callback=request_callback)
    try:
        print("Waiting for requests...")
        channel.start_consuming()
    except KeyboardInterrupt:
        print("Goodbye!")
        sys.exit(0)

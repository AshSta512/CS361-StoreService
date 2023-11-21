import uuid

import pika
import json


class StoreClient:

    def __init__(self):
        self.connection = pika.BlockingConnection(pika.ConnectionParameters("localhost"))
        self.channel = self.connection.channel()
        self.store_data_request_queue = self.channel.queue_declare(queue="store_data_request")

        self.callback_queue = self.store_data_request_queue.method.queue

        self.channel.basic_consume(
            queue=self.callback_queue,
            on_message_callback=self.response_callback,
            auto_ack=True
        )

        self.response = None
        self.correlation_id = None

    def response_callback(self, ch, method, properties, body):
        if self.correlation_id == properties.correlation_id:
            self.response = body.decode()

    def call(self, product_str):
        self.response = ""
        self.correlation_id = str(uuid.uuid4())
        self.channel.basic_publish(exchange="",
                                   routing_key="store_data_request",
                                   properties=pika.BasicProperties(reply_to=self.callback_queue,
                                                                   correlation_id=self.correlation_id),
                                   body=product_str)
        self.connection.process_data_events(time_limit=None)
        return self.response


if __name__ == '__main__':
    store_client = StoreClient()
    while True:
        product_request = input("Next Product Request:\n")
        response = store_client.call(product_request)
        product_list = json.loads(response)
        print(product_list)

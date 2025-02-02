import pika
import logging
rab_params = pika.ConnectionParameters(
    host='localhost',
    port=5672,
    credentials=pika.PlainCredentials('guest', 'guest')
)


logger = logging.getLogger('autotrade.' + __name__)

class RabbitConnection:

    def __init__(self,
                 params=rab_params,
                 ):

        self.connection = pika.BlockingConnection(params)
        self.channel = self.connection.channel()

        logger.info('RabbitMQ connection and channel initialized')



class RabbitConCSV:

    def __init__(self,
                 params=rab_params,
                 exchange_name='exchange_data_handler',
                 exchange_type='topic'
                 ):

        self.connection = pika.BlockingConnection(params)
        self.channel = self.connection.channel()

        self.channel.exchange_declare(
            exchange=exchange_name,
            exchange_type=exchange_type,
            passive=False,
            auto_delete=True
        )
        logger.info('RabbitMQ connection and channel initialized v0')

    def produce(self,
                body: str,
                exchange='exchange_data_handler',
                routing_key='data_csv'
                ):

        self.channel.basic_publish(
            exchange=exchange,
            routing_key=routing_key,
            body=body
        )
        # print(f'''
        #     message sent with body: \n
        #     {body}
        # ''')

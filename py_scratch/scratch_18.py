import sys
import threading
import time
sys.path.insert(0, r'Z:\Benchmarking\py_integra\PyCharmProjects\autotrade')

from libs.rabfile import *


def cllbck(ch, method, properties, body):
    print(body)


def consumer(ch):

    ch.queue_declare(
        queue='q_scratch18',
        passive=False,
    )

    ch.queue_bind(
        queue='q_scratch18',
        exchange='exch_scratch18',
        routing_key='qs8'
    )

    ch.basic_consume(
        queue='q_scratch18',
        on_message_callback=cllbck,
        auto_ack=True
    )

    ch.start_consuming()
    print('consuming stopped')


def publisher(ch, ev):

    while not ev.is_set():
        ch.basic_publish(
            exchange='exch_scratch18',
            routing_key='cola_aea2',
            body=b'mandado desde scratch18'
        )
        print('published')
        time.sleep(5)
    print('published stopped')


rab_con = RabbitConnection()

rab_con.channel2 = rab_con.connection.channel()

rab_con.channel.exchange_declare(
    exchange='exch_scratch18',
    exchange_type='topic',
    passive=False,
)

event_publisher = threading.Event()

t_consumer = threading.Thread(target=consumer, args=(rab_con.channel, ))
t_publisher = threading.Thread(target=publisher, args=(rab_con.channel, event_publisher, ))

t_publisher.start()
t_consumer.start()

#
# t_consumer.join()
# rab_con.connection.add_callback_threadsafe(rab_con.channel.stop_consuming)
#
#
# rab_con.connection.is_open
# event_publisher.set()
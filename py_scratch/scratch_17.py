import polars as pl
import queue
import threading
import time


class Prueba:

    def __init__(self):
        self.queue = queue.Queue()

    def producer(self):
        t_producer = threading.Thread(target=self._producer, args=())
        t_producer.start()

    def _producer(self):
        df = pl.read_parquet(
            r'Z:\Benchmarking\py_integra\PyCharmProjects\autotrade\synthetic_server_path\us_equity.parquet'
        )

        for i in range(4):
            self.queue.put(df)
            time.sleep(5)
        print('no more consuming allowerd')

    def consumer(self):
        a = self.queue.get()
        self.queue.task_done()
        return a


aver = Prueba()

aver.producer()

cc = aver.consumer()

import threading
import time
import datetime as dt
import logging


class LogRecordNs(logging.LogRecord):
    def __init__(self, *args, **kwargs):
        self.ct_ns = time.time_ns()
        self.actual_ns = int((self.ct_ns * 1e9 - int(self.ct_ns * 1e9)) * 1e9)
        super().__init__(*args, **kwargs)


logging.setLogRecordFactory(LogRecordNs)


class ModFormatter(logging.Formatter):
    def formatTime(self, record, datefmt=None):

        if not datefmt:
            super().formatTime(record, datefmt=None)
        else:
            if '.%f' in datefmt:
                datefmt = datefmt.replace('.%f', '')
            out_string = dt.datetime.fromtimestamp(record.ct_ns / 1e9).strftime(datefmt)
            ns_string = str(int((record.ct_ns / 1e9 - int(record.ct_ns / 1e9)) * 1e9))
            return out_string + '.' + ns_string


logger = logging.getLogger('logger_scratch12')
logger.setLevel(logging.DEBUG)

log_handler_file = logging.FileHandler(
    filename=r'Z:\Benchmarking\py_integra\PyCharmProjects\autotrade\scratch12log.log',
    encoding='UTF-8',
    mode='w'
)
log_handler_stdout = logging.StreamHandler()

log_format = ModFormatter(
    fmt='{asctime:<20}---{name:<20}---{levelname:<7}---{message:<40}--{thread:<10}',
    datefmt='%H:%M:%S.%f',
    style='{',
    validate=True
)

log_handler_file.setFormatter(log_format)
log_handler_stdout.setFormatter(log_format)

logger.addHandler(log_handler_file)
logger.addHandler(log_handler_stdout)

quit_flag = threading.Event()

quit_lock = threading.Lock()


def fun_dae():
    i = 0
    # while True and i <10:
    while True:
        with quit_lock as locked_or_not:
            logger.debug(f'quit_lock status: {locked_or_not}')
            logger.debug(f'quit flag is {quit_flag.is_set()}')
            if quit_flag.is_set():
                logger.debug('quit flag activated')
                return
            logger.debug(f'about to release the lock')
        logger.debug(f'fun dae - {threading.current_thread().daemon} - {i + 1}')
        time.sleep(1)
        i += 1


def fun_normal():
    for i in range(3):
        logger.debug(f'fun normal - {threading.current_thread().daemon} - {i + 1}')
        time.sleep(1)


# def quit_daemon():

t1 = threading.Thread(target=fun_dae, args=())
t2 = threading.Thread(target=fun_normal, args=())

t1.daemon = True
# t2.daemon = True
logger.debug(f'main thread is daemon: {threading.current_thread().daemon}')

t1.start()
t2.start()

t2.join()
# time.sleep(2)
logger.debug(f'check: {quit_lock.locked()=}')
logger.debug('pre finished')

# quit_lock.acquire()
#
# quit_lock.release()
#
# quit_flag.set()

with quit_lock as lockeado:
    print(lockeado)
    time.sleep(10)
    quit_flag = True

# def stopper():
#     with quit_lock:
#         quit_flag = True
# stopper()
logger.debug('finished')
# lockeado = quit_lock.acquire()
#
# quit_lock.release()

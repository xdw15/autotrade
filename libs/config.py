from os import getcwd


work_path = getcwd().replace('\\', '/')
db_path = work_path + '/synthetic_server_path'


trading_router = 'ib'

exchange_declarations = {
    'OrderExecution': {"exchange": 'execution_order_exchange',
               "exchange_type": "topic",
               "passive": False},
    'OrderReceiver': {"exchange": "placement_order_exchange",
                       "exchange_type": "topic",
                       "passive": False},
    'DataHandler': {'exchange': 'datahandler_exchange',
                    'exchange_type': 'topic',
                    'passive': False}
}

all_routing_keys = {
    'AutoExecution_server': 'rt_autoexecution_rpc_server',
    'AutoExecution_client': 'rt_autoexecution_rpc_client',
    'AutoPort_DH_endpoint': ['data_csv.*',],
    'AutoPort_OrderReceiver': {'DumbStrat': 'DumbStrat_SignalSubscription'}

}

autoport_tables = ['us_equity',
                   ]

queue_declarations = {
    'AutoPort_OrderReceiver': {'queue': 'AutoPort_OrderReceiver',
                               'passive': False,
                               'auto_delete': True,
                               'exclusive': True},
    'AutoExecution_rpc_server': {'queue': 'rpc_order_router',
                                 'passive': False,
                                 'auto_delete': True,
                                 'exclusive': True,
                                 'arguments': {'x-consumer-timeout': 1*60_000}},
    'AutoPort_DH_endpoint': {'queue': '',
                            'exclusive': True,
                            'auto_delete': True,
                            'passive': False}
}

# hkrkyf760 - @Tomate4
ib_mode = 'live'
ib_account = {'live': 'U9765800',
              'paper': 'DU7219906'}
ibg_connection_params = {
    'port': 4001,
    'clientId': 3,
    'readonly': False,
    'account': ib_account[ib_mode]}

ib_order_kwargs = {'tif': "DAY",
                   "account": ib_account[ib_mode],
                   "clearingIntent": "IB"}







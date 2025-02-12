from os import getcwd


work_path = getcwd().replace('\\', '/')
db_path = work_path + '/synthetic_server_path'

exchange_params = {
    'orders': {"exchange": 'execution_order_exchange',
               "exchange_type": "topic",
               "passive": False}
}
all_routing_keys = {
    'order_consumer': 'rpc_order_consumer',
    'AutoPort_db_endpoint': ['data_csv.*',]
}
ibg_params = {
    'socket_port': 4001,
    'master_api_client_id': 7,
    'allowed_order_types': {'LMT'},
}

ib_order_kwargs = {'tif': "DAY",
                   "account": "U9765800",
                    "clearingIntent": "IB"}


from os import getcwd


work_path = getcwd().replace('\\', '/')
db_path = work_path + '/synthetic_server_path'

exchange_names = {
    'orders': 'execution_order_exchange'
}
routing_keys = {
    'order_consumer': 'rpc_order_consumer',
    'AutoPort_db_endpoint': ['data_csv.*',]
}
ibg_params = {
    'socket_port': 4001,
    'master_api_client_id': 7,
    'allowed_order_types': {'Limit'},
}

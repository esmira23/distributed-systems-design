from random import choice
import consul
import json
import uuid

consul_client = consul.Consul(host="127.0.0.1", port=8500)

# Register service with Consul
def register_service(service_name, service_port):
    service_id=str(uuid.uuid4())
    consul_client.agent.service.register(
        service_name,
        service_id,
        address="localhost",
        port=service_port,
    )
    return service_id

# Deregister service from Consul
def deregister_service(service_name):
    consul_client.agent.service.deregister(service_name)

# Read service addresses from Consul
def get_service_address_port(service_name):
    services_tmp = [
        f"http://{service_info['Address']}:{service_info['Port']}"
        for _, service_info in consul_client.agent.services().items()
        if service_info['Service'] == service_name
    ]
    return choice(services_tmp) if services_tmp else None

def store_key_value(key, value):
    consul_client.kv.put(key, value)

def get_key_value(key):
    _, config = consul_client.kv.get(key)
    return config['Value'] if config else None


hz_configs = {
    "cluster_name": "dev",
    "cluster_members": [
        "127.0.0.1:5701",
        "127.0.0.1:5702",
        "127.0.0.1:5703"
    ],
    "map_name": "messages",
}

mq_configs = {
    "queue_name": "message-queue"
}

hz_configs_json = json.dumps(hz_configs)
store_key_value('hz_configs', hz_configs_json)

mq_configs_json = json.dumps(mq_configs)
store_key_value('mq_configs', mq_configs_json)
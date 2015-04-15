__author__ = 'zephyre'

from kombu import Exchange, Queue

CELERY_ACCEPT_CONTENT = ['json', 'msgpack', 'yaml']
CELERY_QUEUES = (
    Queue('dhaulagiri.default', Exchange('dhaulagiri'), auto_declare=True, routing_key='dhaulagiri'),
    Queue('dhaulagiri.images', Exchange('dhaulagiri'), auto_declare=True, routing_key='images'),
    Queue('dhaulagiri.baidu', Exchange('dhaulagiri'), auto_declare=True, routing_key='baidu')
)
CELERY_DEFAULT_QUEUE = 'dhaulagiri.default'
CELERY_DEFAULT_EXCHANGE_TYPE = 'direct'
CELERY_DEFAULT_ROUTING_KEY = 'dhaulagiri'

CELERY_ROUTES = {
    'processors.images.upload': {
        'exchange': 'dhaulagiri',
        'routing_key': 'images'
    }
}
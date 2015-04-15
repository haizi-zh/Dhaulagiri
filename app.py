from celery import Celery
import celery_config
from utils import load_yaml

__author__ = 'zephyre'

app = Celery('Dhaulagiri', broker=load_yaml()['celery']['broker'])

app.config_from_object(celery_config)



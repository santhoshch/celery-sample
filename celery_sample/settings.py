"""
Django settings for celery_sample project.

For more information on this file, see
https://docs.djangoproject.com/en/1.6/topics/settings/

For the full list of settings and their values, see
https://docs.djangoproject.com/en/1.6/ref/settings/
"""

import djcelery
import sys
djcelery.setup_loader()

reload(sys)
# This is to satisfy static code checking tool.  reload(sys) adds certain
# methods at runtime
sys1 = sys
sys1.setdefaultencoding('utf-8')
# Build paths inside the project like this: os.path.join(BASE_DIR, ...)
import os
BASE_DIR = os.path.dirname(os.path.dirname(__file__))


# Quick-start development settings - unsuitable for production
# See https://docs.djangoproject.com/en/1.6/howto/deployment/checklist/
# 
# # SECURITY WARNING: keep the secret key used in production secret!
# SECRET_KEY = '!3t=hhjsx09x8ly19%-zyf44dl4x82k3p4^3l+3#64)1@mzwf='
SECRET_KEY = '12121121121212'
# 
# # SECURITY WARNING: don't run with debug turned on in production!
DEBUG = True

TEMPLATE_DEBUG = True

ALLOWED_HOSTS = []


# Application definition

INSTALLED_APPS = (
    'django.contrib.admin',
    'django.contrib.auth',
    'django.contrib.contenttypes',
    'django.contrib.sessions',
    'django.contrib.messages',
    'django.contrib.staticfiles',
)

MIDDLEWARE_CLASSES = (
    'django.contrib.sessions.middleware.SessionMiddleware',
    'django.middleware.common.CommonMiddleware',
    'django.middleware.csrf.CsrfViewMiddleware',
    'django.contrib.auth.middleware.AuthenticationMiddleware',
    'django.contrib.messages.middleware.MessageMiddleware',
    'django.middleware.clickjacking.XFrameOptionsMiddleware',
)


# This setting is used by django-jenkins to know which are
# tests written by Gnana
PROJECT_APPS = ('domainmodel', 'celerytasks', 'commands')

BORROWED_APPS = (
    'gunicorn',
    'bootstrap3',
#     'keymanager',
    'djcelery',  # Remove this again when we move to 3.1+
    'south'
)

# Create the complete set of apps to be used
INSTALLED_APPS += BORROWED_APPS
INSTALLED_APPS += PROJECT_APPS

ROOT_URLCONF = 'celery_sample.urls'

WSGI_APPLICATION = 'celery_sample.wsgi.application'

SITE_ROOT = os.path.normpath(
                    os.path.join(
                        os.path.dirname(os.path.realpath(__file__)
                    ), '..'))
CNAME = os.environ.get('GNANA_CNAME', "localhost")

# Database
# https://docs.djangoproject.com/en/1.6/ref/settings/#databases

DATABASES = {
    'default': {
        'ENGINE': 'django.db.backends.sqlite3',
        'NAME': os.path.join(BASE_DIR, 'db.sqlite3'),
    }
}

# Internationalization
# https://docs.djangoproject.com/en/1.6/topics/i18n/

LANGUAGE_CODE = 'en-us'

TIME_ZONE = 'UTC'

USE_I18N = True

USE_L10N = True

USE_TZ = True


# Static files (CSS, JavaScript, Images)
# https://docs.djangoproject.com/en/1.6/howto/static-files/

STATIC_URL = '/static/'

# added newly
TRUE_VALUES = {'true', '1', 'yes', 'yeah', 'si',
               'youbet', 'good', 'positive'}
def is_true(bool_like):
    if(isinstance(bool_like, basestring)):
        return bool_like.lower() in TRUE_VALUES
    if bool_like:
        return True
    return False

mongo_db_url = 'mongodb://gnana:gnana@localhost:27017/gnana'

CELERY_IMPORTS = ['celerytasks.tasker.gtasks']
CELERYD_POOL_RESTARTS = True
CELERY_RESULT_BACKEND = 'mongodb'


import urlparse
mongo_db_url_parsed = urlparse.urlparse(mongo_db_url)
if('mongo-db-name' not in os.environ):
    # Use the DBname from url
    dbname = mongo_db_url_parsed.path.strip('/')
    os.environ['mongo-db-name'] = dbname

BROKER_URL = os.environ.get('CELERY_BROKER', mongo_db_url)

CELERY_MONGODB_BACKEND_SETTINGS = {
    "host": mongo_db_url,
#     "port": mongo_db_url_parsed.port,
#     "user": mongo_db_url_parsed.username,
#     "password": mongo_db_url_parsed.password,
    "database": os.environ.get('mongo-db-name', 'gnana'),
    "taskmeta_collection": "task_results",
}

# ALLOWED_HOSTS = [ '*' ]
# CELERY_ROUTES = {
#         'celerytasks.sample_task': {
#             'queue': 'tasks'
#         },
# }

CELERY_TASK_SERIALIZER = 'json'
CELERY_ACCEPT_CONTENT = ['json', 'pickle', 'application/json', 'application/x-python-serialize']

CELERYD_HIJACK_ROOT_LOGGER = False
if os.environ.get('KEEP_CELERY_ALIVE',False):
    CELERYD_MAX_TASKS_PER_CHILD = 1000
else:
    CELERYD_MAX_TASKS_PER_CHILD = 1
CELERYD_PREFETCH_MULTIPLIER = 1
CELERYD_FORCE_EXECV = True
# 
# CELERYBEAT_SCHEDULER = 'djcelery.schedulers.DatabaseScheduler'
# from datetime import timedelta
# CELERYBEAT_SCHEDULE = {
#     'add-every-60-seconds': {
#         'task': 'celerytasks.sample_task.test',
#         'schedule': timedelta(seconds=120),
#         'options': {'queue' : 'tasks'}
#     },}


BROKER_USE_SSL = is_true(os.environ.get('CELERY_SSL', False))

if BROKER_USE_SSL:
    BROKER_POOL_LIMIT = 1
    CELERY_SECURITY_KEY = os.environ.get('SSL_KEY', None)
    CELERY_SECURITY_CERTIFICATE = os.environ.get('SSL_CERT', None)
    CELERY_SECURITY_CERT_STORE = os.environ.get('SSL_STORE', None)



from logging import Formatter

LOGGING = {
    'version': 1,
    'disable_existing_loggers': False,
    'filters': {
        'require_debug_false': {
            '()': 'django.utils.log.RequireDebugFalse'
        }
    },
    'formatters': {
        'simple': {
            '()': 'logging.Formatter',
            'format': '%(levelname)s [%(process)d] %(message)s'
        },
        'verbose': {
            '()': 'logging.Formatter',
            'format': '%(levelname)s  [%(process)d] %(message)s'
        },
    },
    'handlers': {
        'mail_admins': {
            'level': 'ERROR',
            'filters': ['require_debug_false'],
            'class': 'django.utils.log.AdminEmailHandler'
        },
        'console': {
            'level': 'DEBUG',
            'class': 'logging.StreamHandler',
            'formatter': 'simple'
        },
        'file': {
            'filename': 'test.log',
            'class': 'logging.handlers.WatchedFileHandler',
            'formatter': 'verbose',
        }
    },
    'loggers': {
        '': {
             'handlers': ['console'],
             'level': 'ERROR',
         },
        'django.request': {
            'handlers': ['mail_admins'],
            'level': 'ERROR',
            'propagate': True,
        },
        'gnana': {
            'handlers': ['console'],  # 'syslog'],
            'level': 'INFO',
            'propagate': False,
        },
        'celery': {
            'handlers': ['console'],  # 'syslog'],
            'level': 'INFO',
            'propagate': False,
        }
    }
}

LOGGING['loggers']['gnana']['handlers'].extend(['file'])
LOGGING['loggers']['celery']['handlers'].extend(['file'])
LOGGING['loggers']['']['handlers'].extend(['file'])

from logging.config import dictConfig
dictConfig(LOGGING)

#
# Call a localfile execution to change settings
#
if os.path.exists(os.path.join(SITE_ROOT, "local", "settings.py")):
    print "local/settings.py detected.  Loading local settings module."
    execfile('local/settings.py')

#
#  PERFORM ALL INITIALIZATION HERE
#
#  Call any methods required for initialization here
#
import logging

logger = logging.getLogger('gnana.%s' % __name__)

import time

from pymongo import MongoClient, MongoReplicaSetClient
from mongodb import GnanaMongoDB
for i in range(5):
    try:
        logger.info("Attempting mongo connection....")
        mongo_con = MongoClient(mongo_db_url)[os.environ['mongo-db-name']]
        RS_NAME = os.environ.get('RS_NAME', None)
        if RS_NAME is not None:
            mongo_con = MongoReplicaSetClient(mongo_db_url, replicaSet=RS_NAME)[mongo_db_url.split('/')[-1]]
        gnana_db = GnanaMongoDB(mongo_con)
    except Exception as e:
        logger.exception("Unable to connect with the main mongodb: " + str(e))
        gnana_db = None
    if gnana_db is not None:
        break
    logger.info("Retrying Mongodb Connection " + str(gnana_db))
    time.sleep(i*5)
    
if not DEBUG and gnana_db is None:
    logger.info("Exiting as we are unable to connect with mongodb")
    sys.exit(1)
else:
    logger.info('Mongo is available')
    
pathprefix = os.environ['HOME'] + '/tenants'
from utils.storage import GnanaFileStorage
gnana_storage = GnanaFileStorage()

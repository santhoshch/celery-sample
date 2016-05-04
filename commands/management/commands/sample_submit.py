'''
Created on 02-May-2016

@author: ravi
'''
from django.core.management import BaseCommand
import logging
import datetime
import pytz

logger = logging.getLogger('gnana.%s' % __name__)


class Command(BaseCommand):

    def handle(self, *args, **options):
        # results
        print ' in sample submit command'
        try:
            from celerytasks.framework import submitasync
#             from celerytasks.sample_task import test_task
            from celerytasks.tasker.gtasks import run_master_task
            logger.info("submitting a sample task")
            options['path'] = 'celerytasks.sample_tasks.Level'
            return submitasync(run_master_task, queue='tasks', **options)
#
#             from celerytasks.tasker.helpers import run_celery_task
#             user_and_tenant = ('user', 'tenant', 'login_tenant')
#             params = {}
#             context = {'task_pool':'worker'}
#             return run_celery_task(user_and_tenant, Level, params, context)

        except Exception as ex:
            logger.exception('na')

from random import random
from time import time, strftime, gmtime, sleep

from taskframework.framework import V2Task
import logging
from django.utils.functional import cached_property

logger = logging.getLogger('gnana.%s' % __name__)


class Level(V2Task):

    def execute(self, dep_results):
        print('------------------STARTING ------------------- %s - %s' % (self.name, strftime("%a, %d %b %Y %H:%M:%S +0000", gmtime())))
        print('Process output for class %s - dep outputs %s ' %
              (self.name, dep_results))
        temp = random() * 10
        sleep(temp)
        self.output = '%s_%s' % (self.name, temp)
        print('------------------FINISHING ------------------- %s - %s' % (self.name, strftime("%a, %d %b %Y %H:%M:%S +0000", gmtime())))

    @cached_property
    def dependencies(self):
        overrides = self.__dict__
        overrides.update({'t':'Level0'})
        deps = {'Level_0_0': Level_0(**overrides), 'Level_1_0': Level_1(**overrides), }
        return deps


class Level_1_0(V2Task):

    def execute(self, dep_results):
        print('------------------STARTING ------------------- %s - %s' % (self.name, strftime("%a, %d %b %Y %H:%M:%S +0000", gmtime())))
        print('Process output for class %s - dep outputs %s ' %
              (self.name, dep_results))
        temp = random() * 10
        sleep(temp)
        self.output = '%s_%s' % (self.name, temp)
        print('------------------FINISHING ------------------- %s - %s' % (self.name, strftime("%a, %d %b %Y %H:%M:%S +0000", gmtime())))

    @cached_property
    def dependencies(self):
        overrides = self.__dict__
        overrides.update({'t':'Level_1_01'})
        deps = {}
        return deps


class Level_1_1(V2Task):

    def execute(self, dep_results):
        print('------------------STARTING ------------------- %s - %s' % (self.name, strftime("%a, %d %b %Y %H:%M:%S +0000", gmtime())))
        print('Process output for class %s - dep outputs %s ' %
              (self.name, dep_results))
        temp = random() * 10
        sleep(temp)
        self.output = '%s_%s' % (self.name, temp)
        print('------------------FINISHING ------------------- %s - %s' % (self.name, strftime("%a, %d %b %Y %H:%M:%S +0000", gmtime())))

    @cached_property
    def dependencies(self):
        overrides = self.__dict__
        overrides.update({'t':'Level_1_12'})
        deps = {}
        return deps


class Level_0_1(V2Task):

    def execute(self, dep_results):
        print('------------------STARTING ------------------- %s - %s' % (self.name, strftime("%a, %d %b %Y %H:%M:%S +0000", gmtime())))
        print('Process output for class %s - dep outputs %s ' %
              (self.name, dep_results))
        temp = random() * 10
        sleep(temp)
        self.output = '%s_%s' % (self.name, temp)
        print('------------------FINISHING ------------------- %s - %s' % (self.name, strftime("%a, %d %b %Y %H:%M:%S +0000", gmtime())))

    @cached_property
    def dependencies(self):
        overrides = self.__dict__
        overrides.update({'t':'Level_0_13'})
        deps = {}
        return deps


class Level_0_0(V2Task):

    def execute(self, dep_results):
        print('------------------STARTING ------------------- %s - %s' % (self.name, strftime("%a, %d %b %Y %H:%M:%S +0000", gmtime())))
        print('Process output for class %s - dep outputs %s ' %
              (self.name, dep_results))
        temp = random() * 10
        sleep(temp)
        self.output = '%s_%s' % (self.name, temp)
        print('------------------FINISHING ------------------- %s - %s' % (self.name, strftime("%a, %d %b %Y %H:%M:%S +0000", gmtime())))

    @cached_property
    def dependencies(self):
        overrides = self.__dict__
        overrides.update({'t':'Level_0_04'})
        deps = {}
        return deps

class Level_0(V2Task):

    def execute(self, dep_results):
        print('------------------STARTING ------------------- %s - %s' % (self.name, strftime("%a, %d %b %Y %H:%M:%S +0000", gmtime())))
        print('Process output for class %s - dep outputs %s ' %
              (self.name, dep_results))
        temp = random() * 10
        sleep(temp)
        self.output = '%s_%s' % (self.name, temp)
        print('------------------FINISHING ------------------- %s - %s' % (self.name, strftime("%a, %d %b %Y %H:%M:%S +0000", gmtime())))

    @cached_property
    def dependencies(self):
        overrides = self.__dict__
        overrides.update({'t':'Level_05'})
        deps = {'Level_0_1_0': Level_0_1(**overrides), 'Level_0_0_0': Level_0_0(**overrides), }
        return deps


class Level_1(V2Task):

    def execute(self, dep_results):
        print('------------------STARTING ------------------- %s - %s' % (self.name, strftime("%a, %d %b %Y %H:%M:%S +0000", gmtime())))
        print('Process output for class %s - dep outputs %s ' %
              (self.name, dep_results))
        temp = random() * 10
        sleep(temp)
        self.output = '%s_%s' % (self.name, temp)
        print('------------------FINISHING ------------------- %s - %s' % (self.name, strftime("%a, %d %b %Y %H:%M:%S +0000", gmtime())))

    @cached_property
    def dependencies(self):
        overrides = self.__dict__
        overrides.update({'t':'Level_16'})
        deps = {'Level_1_0_0': Level_1_0(**overrides),
                'Level_1_1_0': Level_1_1(**overrides), }
        return deps

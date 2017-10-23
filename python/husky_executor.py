#!/usr/bin/env python

# Copyright 2017 Husky Team
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import sys
import threading
import time

import mesos.interface
from mesos.interface import mesos_pb2
import mesos.native

class HuskyExecutor(mesos.interface.Executor):
    def registered(self, driver, executorInfo, frameworkInfo, slaveInfo):
        print "HuskyExecutor registered"

    def reregistered(self, driver, slaveInfo):
        print "HuskyExecutor reregistered"

    def disconnected(self, driver):
        print "HuskyExecutor disconnected"

    def launchTask(self, driver, task):
        def run_task():
            print "Running task %s" % task.task_id.value
            update = mesos_pb2.TaskStatus()
            update.task_id.value = task.task_id.value
            update.state = mesos_pb2.TASK_RUNNING
            update.data = "run"
            driver.sendStatusUpdate(update)

            print "Test"
            update = mesos_pb2.TaskStatus()
            update.task_id.value = task.task_id.value
            update.state = mesos_pb2.TASK_FINISHED
            update.data = "done"
            driver.sendStatusUpdate(update)

        thread = threading.Thread(target=run_task)
        thread.start()

    def frameworkMessage(self, driver, message):
        driver.sendFrameworkMessage(message)

    def killTask(self, driver, taskId):
        print "Shutdown task %s" % taskId

    def error(self, error, message):
        pass

if __name__ == "__main__":
    print "Starting HuskyExecutor"
    driver = mesos.native.MesosExecutorDriver(HuskyExecutor())
    sys.exit(0 if driver.run() == mesos_pb2.DRIVER_STOPPED else 1)

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

import os
import sys
import time

try:
    import mesos.interface
    from mesos.interface import mesos_pb2
    import mesos.native
except ImportError as e:
    print(e)
    sys.exit(1)

PYTHON_EXECUTABLE = sys.executable
CURRENT_PATH = os.getcwd()


class HuskyScheduler(mesos.interface.Scheduler):
    def __init__(self, executor):
        self.executor = executor
        self.done = False
        self.taskData = {}
        self.messagesReceived = 0

    def registered(self, driver, frameworkId, masterInfo):
        print "Registered with framework ID %s" % frameworkId.value

    def makeTaskPrototype(self, offer, name):
        task = mesos_pb2.TaskInfo()
        task.task_id.value = str(name)
        task.slave_id.value = offer.slave_id.value
        task.name = "start worker task %s" % name

        cpus = task.resources.add()
        cpus.name = "cpus"
        cpus.type = mesos_pb2.Value.SCALAR
        cpus.scalar.value = 1

        mem = task.resources.add()
        mem.name = "mem"
        mem.type = mesos_pb2.Value.SCALAR
        mem.scalar.value = 32

        task.executor.MergeFrom(self.executor)

        return task

    def resourceOffers(self, driver, offers):
        for offer in offers:
            if self.done:
                print "Shutting down: declining offer on [%s]" % offer.hostname
                driver.declineOffer(offer.id)
                continue

            tasks = []

            if not self.done:
                master_task = self.makeTaskPrototype(offer, "Master")
                master_task.data = "./Master --conf config.ini"
                self.taskData[master_task.task_id.value] = (offer.slave_id, master_task.executor.executor_id)

                worker_task = self.makeTaskPrototype(offer, "Worker")
                worker_task.data = "./PI --conf config.ini"
                self.taskData[worker_task.task_id.value] = (offer.slave_id, worker_task.executor.executor_id)

                tasks += [master_task, worker_task]
                self.done = True

            if tasks:
                print "Accepting offer on [%s]" % offer.hostname
                driver.launchTasks(offer.id, tasks)
            else:
                print "Declining offer on [%s]" % offer.hostname
                driver.declineOffer(offer.id)

    def statusUpdate(self, driver, update):
        print "task %s is in state %s" % (update.task_id.value, mesos_pb2.TaskState.Name(update.state))

        if update.data != "run" and update.data != "done":
            print "Wrong message: %s" % str(update.data)
            sys.exit(1)

        if update.state == mesos_pb2.TASK_FINISHED:
            slave_id, executor_id = self.taskData[update.task_id.value]
            driver.sendFrameworkMessage(executor_id, slave_id, "Finished")

        if update.state == mesos_pb2.TASK_LOST or \
           update.state == mesos_pb2.TASK_KILLED or \
           update.state == mesos_pb2.TASK_FAILED:
            driver.abort()

    def frameworkMessage(self, driver, executorId, slaveId, message):
        self.messagesReceived += 1
        if message != "Finished":
            print "Wrong message: %s" % str(message)
            sys.exit(1)

        print "received msg:", repr(str(message))

        if self.messagesReceived == 2:
            driver.stop()

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print "Usage: %s master" % sys.argv[0]
        sys.exit(1)

    uris = [os.path.join(CURRENT_PATH, "husky", uri) for uri in ["Master", "PI", "config.ini"]]
    uris.append(os.path.join(CURRENT_PATH, "husky_executor.py"))

    executor = mesos_pb2.ExecutorInfo()
    executor.executor_id.value = "husky-executor"
    executor.name = "Husky Executor (Python)"
    executor.command.value = "%s husky_executor.py" % PYTHON_EXECUTABLE

    for uri in uris:
        uri_proto = executor.command.uris.add()
        uri_proto.value = uri
        uri_proto.extract = False

    framework = mesos_pb2.FrameworkInfo()
    framework.user = "" # Have Mesos fill in the current user.
    framework.name = "Husky Framework (Python)"
    framework.checkpoint = True

    driver = mesos.native.MesosSchedulerDriver(
        HuskyScheduler(executor),
        framework,
        sys.argv[1])

    status = 0 if driver.run() == mesos_pb2.DRIVER_STOPPED else 1

    driver.stop();

    sys.exit(status)

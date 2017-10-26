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

MASTER_PORT = 14517
COMM_PORT = 14818

HDFS_NAMENODE = ('master', '9000')

class HuskyScheduler(mesos.interface.Scheduler):
    def __init__(self, executor, cpus=1, mems=32):
        self.executor = executor
        self.cpus = cpus
        self.mems = mems

        self.finished = False
        self.task_data = dict()
        self.messages_received = 0

        self.master_host = None
        self.workers = set()

    def registered(self, driver, framework_id, master_info):
        print "Registered with framework ID %s" % framework_id.value

    def makeTaskPrototype(self, offer, name):
        task = mesos_pb2.TaskInfo()
        task.task_id.value = str(name)
        task.slave_id.value = offer.slave_id.value
        task.name = "start worker task %s" % name

        cpus = task.resources.add()
        cpus.name = "cpus"
        cpus.type = mesos_pb2.Value.SCALAR
        cpus.scalar.value = self.cpus

        mem = task.resources.add()
        mem.name = "mem"
        mem.type = mesos_pb2.Value.SCALAR
        mem.scalar.value = self.mems

        task.executor.MergeFrom(self.executor)

        return task

    def resourceOffers(self, driver, offers):
        if len(self.workers) == 0:
            for offer in offers:
                self.workers.add(offer.hostname.encode('utf-8'))
        worker_info = ['%s:%s' % (worker, self.cpus) for worker in sorted(self.workers)]

        worker_count = 0
        for offer in offers:
            if self.finished:
                continue

            tasks = []
            hostname = offer.hostname.encode('utf-8')
            if self.master_host is None:
                master_task = self.makeTaskPrototype(offer, "Master-%s" % hostname)
                master_task.data = "./Master --master_host={0} --master_port={1} --comm_port={2} --worker.info {3} --log_dir=. --serve=0 --hdfs_namenode={4} --hdfs_namenode_port={5}".format(hostname, MASTER_PORT, COMM_PORT, ' '.join(worker_info), HDFS_NAMENODE[0], HDFS_NAMENODE[1])
                self.master_host = hostname
                self.task_data[master_task.task_id.value] = (offer.slave_id, master_task.executor.executor_id)
                tasks.append(master_task)

            worker_task = self.makeTaskPrototype(offer, "Worker-%s" % hostname)
            worker_task.data = "./PI --master_host={0} --master_port={1} --comm_port={2} --worker.info {3} --log_dir=.".format(self.master_host, MASTER_PORT, COMM_PORT, ' '.join(worker_info))
            self.task_data[worker_task.task_id.value] = (offer.slave_id, worker_task.executor.executor_id)

            tasks.append(worker_task)

            if tasks:
                print "Accepting offer on [%s]" % offer.hostname
                driver.launchTasks(offer.id, tasks)
                worker_count += 1

            if worker_count == len(self.workers):
                self.finished = True

    def statusUpdate(self, driver, update):
        print "task %s is in state %s" % (update.task_id.value, mesos_pb2.TaskState.Name(update.state))

        if update.data != "run" and update.data != "done":
            print "Wrong message: %s" % str(update.data)
            sys.exit(1)

        if update.state == mesos_pb2.TASK_FINISHED:
            slave_id, executor_id = self.task_data[update.task_id.value]
            driver.sendFrameworkMessage(executor_id, slave_id, "Finished")

        if update.state == mesos_pb2.TASK_LOST or \
           update.state == mesos_pb2.TASK_KILLED or \
           update.state == mesos_pb2.TASK_FAILED:
            driver.abort()

    def frameworkMessage(self, driver, executor_id, slave_id, message):
        self.messages_received += 1
        if message != "Finished":
            print "Wrong message: %s" % str(message)
            sys.exit(1)

        print "received msg:", repr(str(message))

        if self.messages_received == len(self.workers) + 1:
            driver.stop()


if __name__ == "__main__":
    if len(sys.argv) != 5:
        print "Usage: %s mesos-master cpus(Integer) mems(Integer-MB) path-to-resources" % sys.argv[0]
        sys.exit(1)

    cpus, mems = int(sys.argv[2]), int(sys.argv[3])
    path_to_resources = sys.argv[4]

    cp_cmd = 'cp %s/husky_executor.py %s' % (os.getcwd(), path_to_resources)
    print cp_cmd
    os.system(cp_cmd)
    uris = [os.path.join(path_to_resources, uri) for uri in ["husky_executor.py", "Master", "PI"]]

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
        HuskyScheduler(executor, cpus, mems),
        framework,
        sys.argv[1])

    status = 0 if driver.run() == mesos_pb2.DRIVER_STOPPED else 1

    driver.stop();

    sys.exit(status)

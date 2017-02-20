# Copyright (C) 2014 OpenStack Foundation
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
# implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import json
import logging
import threading
import time

import fixtures

from nodepool import jobs
from nodepool import tests
from nodepool import nodedb
import nodepool.fakeprovider
import nodepool.nodepool


class TestNodepool(tests.DBTestCase):
    log = logging.getLogger("nodepool.TestNodepool")

    def test_db(self):
        db = nodedb.NodeDatabase(self.dburi)
        with db.getSession() as session:
            session.getNodes()

    def test_disabled_label(self):
        """Test that an image and node are not created"""
        configfile = self.setup_config('node_disabled_label.yaml')
        pool = self.useNodepool(configfile, watermark_sleep=1)
        self._useBuilder(configfile)
        pool.start()
        self.waitForImage('fake-provider', 'fake-image')
        self.waitForNodes(pool)

        with pool.getDB().getSession() as session:
            nodes = session.getNodes(provider_name='fake-provider',
                                     label_name='fake-label',
                                     target_name='fake-target',
                                     state=nodedb.READY)
            self.assertEqual(len(nodes), 0)

    def test_job_end_event(self):
        """Test that job end marks node delete"""
        configfile = self.setup_config('node.yaml')
        pool = self.useNodepool(configfile, watermark_sleep=1)
        self._useBuilder(configfile)
        pool.start()
        self.waitForImage('fake-provider', 'fake-image')
        self.waitForNodes(pool)

        msg_obj = {'name': 'fake-job',
                   'build': {'node_name': 'fake-label-fake-provider-1',
                             'status': 'SUCCESS'}}
        json_string = json.dumps(msg_obj)
        # Don't delay when deleting.
        self.useFixture(fixtures.MonkeyPatch(
            'nodepool.nodepool.DELETE_DELAY',
            0))
        handler = nodepool.nodepool.NodeUpdateListener(pool,
                                                       'tcp://localhost:8881')
        handler.handleEvent('onFinalized', json_string)
        self.wait_for_threads()

        with pool.getDB().getSession() as session:
            node = session.getNode(1)
            self.assertEqual(node, None)

    def _test_job_auto_hold(self, result):
        configfile = self.setup_config('node.yaml')
        pool = self.useNodepool(configfile, watermark_sleep=1)
        self._useBuilder(configfile)
        pool.start()

        self.waitForImage('fake-provider', 'fake-image')
        self.waitForNodes(pool)

        with pool.getDB().getSession() as session:
            session.createJob('fake-job', hold_on_failure=1)

        msg_obj = {'name': 'fake-job',
                   'build': {'node_name': 'fake-label-fake-provider-1',
                             'status': result}}
        json_string = json.dumps(msg_obj)
        # Don't delay when deleting.
        self.useFixture(fixtures.MonkeyPatch(
            'nodepool.nodepool.DELETE_DELAY',
            0))
        handler = nodepool.nodepool.NodeUpdateListener(pool,
                                                       'tcp://localhost:8881')
        handler.handleEvent('onFinalized', json_string)
        self.wait_for_threads()
        return pool


class TestGearClient(tests.DBTestCase):
    def test_wait_for_completion(self):
        wj = jobs.WatchableJob('test', 'test', 'test')

        def call_on_completed():
            time.sleep(.2)
            wj.onCompleted()

        t = threading.Thread(target=call_on_completed)
        t.start()
        wj.waitForCompletion()

    def test_handle_disconnect(self):
        class MyJob(jobs.WatchableJob):
            def __init__(self, *args, **kwargs):
                super(MyJob, self).__init__(*args, **kwargs)
                self.disconnect_called = False

            def onDisconnect(self):
                self.disconnect_called = True
                super(MyJob, self).onDisconnect()

        client = nodepool.nodepool.GearmanClient()
        client.addServer('localhost', self.gearman_server.port)
        client.waitForServer()

        job = MyJob('test-job', '', '')
        client.submitJob(job)

        self.gearman_server.shutdown()
        job.waitForCompletion()
        self.assertEqual(job.disconnect_called, True)

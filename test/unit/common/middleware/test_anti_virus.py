# Copyright (c) 2012 OpenStack Foundation
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

import array
import json
import unittest
from tempfile import mkdtemp
from shutil import rmtree
from cStringIO import StringIO

import os
import mock
from swift.common import ring, utils
from swift.common.utils import split_path
from swift.common.swob import Request, Response
from swift.common.middleware import anti_virus
from swift.common.storage_policy import StoragePolicy, POLICIES
from test.unit import patch_policies

def log_to_file(log):
	log_file = "/tmp/swift_test.log"
	f = open(log_file, "a+")
	f.write(log)
	f.write("\n^^^^^^^^^^^^^\n")
	f.close()
 
class FakeApp(object):
    def __call__(self, env, start_response):
        return Response(body="FakeApp")(env, start_response)

class TestSwiftAntiVirus(unittest.TestCase):
	def setUp(self):
		#self.app = FakeApp()
		#self.list_endpoints = list_endpoints.filter_factory(
		#{'swift_dir': self.testdir})(self.app)
		logstr = "setUp:- Python AV Middleware tests..."
		log_to_file(logstr)
		self.app = anti_virus.AntiVirusMiddleware(FakeApp(), {})

	def test_simple_request(self):
		resp = Request.blank('/', environ={'REQUEST_METHOD': 'GET',}).get_response(self.app)
		logstr = "test_simple_request:-\n\tResponse Body = {}".format(resp.body)
		log_to_file(logstr)
		self.assertEqual(resp.body, "FakeApp")

	def test_put_empty(self):
		resp = Request.blank('/v1/AUTH_test/container1/object1', environ={'REQUEST_METHOD': 'PUT',}).get_response(self.app)
		logstr = "test_put_empty:-\n\tResponse:- Body: {}".format(resp.body)
		log_to_file(logstr)
		self.assertRegexpMatches(resp.body, "OK")

	def test_put_no_virus(self):
		resp = Request.blank('/v1/AUTH_test/container1/object2', environ={'REQUEST_METHOD': 'PUT', 'wsgi.input': StringIO('foobar')}).get_response(self.app)
		logstr = "test_put_no_virus:-\n\tResponse:- Body: {}".format(resp.body)
		log_to_file(logstr)
		self.assertRegexpMatches(resp.body, "OK")

	def test_put_virus(self):
		import clamd
		resp = Request.blank('/v1/AUTH_test/container1/object3', environ={'REQUEST_METHOD': 'PUT', 'wsgi.input': StringIO(clamd.EICAR)}).get_response(self.app)
		logstr = "test_put_virus:-\n\tResponse:- Body: {}".format(resp.body)
		log_to_file(logstr)
		self.assertRegexpMatches(resp.body, "FOUND")

if __name__ == '__main__':
	suite = unittest.TestLoader().loadTestsFromTestCase(TestSwiftAntiVirus)
	unittest.TextTestRunner(verbosity=2).run(suite)
	

	

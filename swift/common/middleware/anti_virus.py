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

import json

from six.moves.urllib.parse import quote, unquote

from swift.common.ring import Ring
from swift.common.utils import get_logger, split_path
from swift.common.swob import Request, Response
from swift.common.swob import HTTPBadRequest, HTTPMethodNotAllowed
from swift.common.storage_policy import POLICIES
from swift.proxy.controllers.base import get_container_info

import clamd
from io import BytesIO


class AntiVirusMiddleware(object):

    def __init__(self, app, conf):
		cd = clamd.ClamdUnixSocket()
		self.app = app

    def __call__(self, env, start_response):
		if env['REQUEST_METHOD'] == "PUT":
			scan = clamd.ClamdUnixSocket().instream(BytesIO(env['wsgi.input'].read()))
			print "Virus Scan : {}".format(scan)
			if scan:
				return Response(status=403,
								body="Virus {} detected".format(scan['stream']),
								content_type="text/plain")(env, start_response)
		return self.app(env, start_response) 


def filter_factory(global_conf, **local_conf):
    conf = global_conf.copy()
    conf.update(local_conf)

    def anti_virus_filter(app):
        return AntiVirusMiddleware(app, conf)

    return anti_virus_filter

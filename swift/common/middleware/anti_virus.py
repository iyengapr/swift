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
from swift.common.utils import get_logger, split_path, close_if_possible
from webob import Request, Response
#from swift.common.swob import Request, Response
from swift.common.wsgi import make_subrequest
import pyclamd
from io import BytesIO
from cStringIO import StringIO

READ_CHUNK_SIZE = 10*1024*1024 # 10 MiB

def read_in_chunks(file_object, chunk_size = READ_CHUNK_SIZE):

    while True:
        data = file_object.read(chunk_size)
        if not data:
            break
        yield data



class AntiVirusMiddleware(object):

    def __init__(self, app, conf):
        self.app = app
        self.conf = conf
        self.cd = pyclamd.ClamdUnixSocket()

    def __call__(self, env, start_response):
        request = Request(env)

        # Make request body seekable
        request.make_body_seekable()

        try:
            (version, account, container, obj) = split_path(request.path_info, 1, 4, True)
        except ValueError:
            response = request.get_response(self.app)
            return response(env, start_response) # Pass onto the pipeline

        # Skip Anti-virus scan for any container with no objects
        if not obj or (request.method != "PUT"):
            response = request.get_response(self.app)
            return response(env, start_response) # Pass onto the pipeline

        # Scan objects uploaded to the container
        first_copy_wsgi_input_body = request.body_file

        if len(request.body) < READ_CHUNK_SIZE:
            scan = self.cd.scan_stream(first_copy_wsgi_input_body)
            if (scan):
                return Response(status=403,
                    body="Virus detected.\n{}".format(scan['stream']),
                    content_type="text/plain")(env, start_response)
        else:
            # Read file_contents in chunks and scan for virus
            for chunk in read_in_chunks(first_copy_wsgi_input_body):
                scan = self.cd.scan_stream(chunk)
                if (scan):
                    return Response(status=403,
                        body="Virus detected.\n{}".format(scan['stream']),
                        content_type="text/plain")(env, start_response)

        request.body_file.seek(0)
        first_copy_wsgi_input_body = request.body_file
        env['wsgi.input'] = first_copy_wsgi_input_body
        response = request.get_response(self.app)
        return response(env, start_response) # Pass onto the pipeline

def filter_factory(global_conf, **local_conf):
    conf = global_conf.copy()
    conf.update(local_conf)

    def anti_virus_filter(app):
        return AntiVirusMiddleware(app, conf)

    return anti_virus_filter

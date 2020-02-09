#!/usr/bin/env python3


import cgi
import http.server
import io
import mimetypes
import os
import socketserver
import sys
import threading
import time
import urllib

import conductor_web
import conductor_backend

if len(sys.argv) != 2:
    print(f'Usage: {sys.argv[0]} SCALE_FACTOR', file=sys.stderr)
    sys.exit(1)
scale_factor = sys.argv[1]

# rest server config
hostName = "localhost"
serverPort = 8080

# backend config
explainer_mapi_url = 'mapi:monetdb://localhost:50000/SF-0_01'
minion_mapi_url = f'mapi:monetdb://HOSTNAME:50000/SF-{scale_factor.replace(".", "_")}'

# left here by Ansible who got it from Terraform
cluster_name = open(os.path.expanduser('~/.cluster_name')).read().strip()

small_pool_filter = dict(
    cluster=cluster_name,
    cluster_groups='minions',
    size='small')
large_pool_filter = dict(
    cluster=cluster_name,
    cluster_groups='minions',
    size='large')

backend = conductor_backend.make_backend(
    conductor_backend.Connector(explainer_mapi_url),
    conductor_backend.Connector(minion_mapi_url),
    dict(SMALL=small_pool_filter, LARGE=large_pool_filter),
)


class ThreadingSimpleServer(socketserver.ThreadingMixIn, http.server.HTTPServer):
    pass


if __name__ == "__main__":
    restserver = ThreadingSimpleServer(
        (hostName, serverPort), conductor_web.ConductorRequestHandler)
    restserver.backend = backend
    print("Server started http://%s:%s" % (hostName, serverPort))

    try:
        restserver.serve_forever()
    except KeyboardInterrupt:
        pass

    restserver.server_close()
    print("Server stopped.")


import cgi
import json
import mimetypes
import os
import urllib
from http.server import BaseHTTPRequestHandler


class ClientError(Exception):
    """Exception indicating that the client did something wrong."""

    def __init__(self, code, msg):
        super().__init__(msg)
        self.code = code


class ConductorRequestHandler(BaseHTTPRequestHandler):
    """This class handles all incoming http requests and hands them off to
    the back end
    """

    response_sent = False

    # suppress the logging
    def log_message(self, format, *args):
        return

    def send_response(self, *args):
        """Wrapper around super.send_response which also sets self.response_sent to True"""

        ret = super().send_response(*args)
        self.response_sent = True
        return ret

    def getparms(self):
        """Extract POST parameters from the request

        Return them as dictionary str->str.
        Parameters with multiple values trigger an error.
        Parameters which are not UTF-8 decodable trigger an error too.
        """

        content_type_header = self.headers.get('Content-Type')
        if not content_type_header:
            raise ClientError(400, "request has no content type header")

        ctype, pdict = cgi.parse_header(content_type_header)
        if ctype == 'multipart/form-data':
            pdict['boundary'] = bytes(pdict['boundary'], "utf-8")  # py3 fix
            postvars = cgi.parse_multipart(self.rfile, pdict)
        elif ctype == 'application/x-www-form-urlencoded':
            length_header = self.headers.get('Content-Length')
            if length_header:
                length = int(self.headers.get('content-length'))
                content = self.rfile.read(length)
            else:
                content = self.rfile.read()
            postvars = cgi.parse_qs(content, keep_blank_values=1)
        elif ctype == 'application/json' or ctype == 'text/json':
            length_header = self.headers.get('Content-Length')
            if length_header:
                length = int(self.headers.get('content-length'))
                content = self.rfile.read(length)
            else:
                content = self.rfile.read()
            postvars = dict((key, [value])
                            for key, value in json.loads(content).items())
        else:
            raise ClientError(400,
                              'Only application/x-www-form-urlencoded and multipart/form-data are supported')

        # With /form-data, the values are byte objects.
        # With /x-www-form-urlencoded, everything is a byte object.
        # Decode everything and abort on duplicates.
        result = dict()
        for key, values in postvars.items():
            try:
                if type(key) == bytes:
                    key = key.decode('utf-8')
                if len(values) != 1:
                    raise ClientError(
                        400, f"Parameter {key} has {len(values)} values, should be 1")
                value = values[0]
                if type(value) == bytes:
                    value = value.decode()
                result[key] = value
            except UnicodeDecodeError:
                raise ClientError(
                    400, f"Invalid utf-8 in parameter {repr(key)}")

        return result

    def serve_static(self):
        """Act as a simple static files server"""

        path_component = urllib.parse.urlparse(self.path).path
        assert path_component.startswith('/')
        path_component = path_component[1:]
        path = 'static'
        for part in path_component.split('/'):
            if part == '.' or part == '..':
                raise ClientError(400, f'invalid path')
            path = os.path.join(path, part)
        if path.endswith('/'):
            path += "index.html"

        if os.path.isfile(path):
            content = open(path, "rb").read()
            typ, enc = mimetypes.guess_type(path)
            self.send_response(200)
            self.send_header("Content-Type", typ or "text/plain")
            if enc:
                self.send_header("Content-Encoding", enc)
            self.end_headers()
            self.wfile.write(content)
        else:
            raise ClientError(404, f'No such file: {path}')

    def run_protected(self, f):
        try:
            f()
        except ClientError as e:
            if self.response_sent:
                print(f"Client Error but response has already been sent: {e}")
            else:
                self.send_response(e.code)
                self.send_header('Content-Type', 'text/plain; charset=utf-8')
                self.end_headers()
                self.wfile.write(bytes(str(e), 'utf-8'))
                self.wfile.write(b"\n")
                print(f"Client Error: {e}")
        except Exception as e:
            if not self.response_sent:
                self.send_response(500)
                self.send_header('Content-Type', 'text/plain; charset=utf-8')
                self.end_headers()
                self.wfile.write(bytes(f'Exception: {str(e)}\n', 'utf-8'))
                import traceback
                traceback.print_exc()

    def do_GET(self):
        self.run_protected(self.do_GET_inner)

    def do_POST(self):
        self.run_protected(self.do_POST_inner)

    def do_GET_inner(self):
        path = urllib.parse.urlparse(self.path).path
        if path == '/status/':
            self.handle_get_status()
            return
        else:
            self.serve_static()

    def do_POST_inner(self):
        path = urllib.parse.urlparse(self.path).path
        if path == '/query/':
            self.handle_query()
            return
        elif path == '/poolsize/':
            self.handle_poolsize()
        elif path == '/status/':
            self.handle_post_status()
        else:
            raise ClientError(404, "No such endpoint: " + path)

    def handle_query(self):
        parms = self.getparms()
        query = parms.get('query')
        if not query:
            raise ClientError(400, "Must provide query")

        result = self.server.backend.execute_query(query)

        resp = json.dumps(result, indent=4)
        self.send_response(200)
        self.send_header('Content-Type', 'application/json; charset=utf-8')
        self.end_headers()
        self.wfile.write(bytes(resp, 'utf-8'))
        self.wfile.write(b"\n")

    def handle_poolsize(self):
        parms = self.getparms()
        for poolname, size in parms.items():
            try:
                size = int(size)
            except ValueError:
                raise ClientError(400, f"Can't parse size {size}")
            if size < 0:
                raise ClientError(400, f"size must be >= 0")
            self.server.backend.set_pool_size(poolname, size)

        self.send_response(200)
        self.send_header('Content-Type', 'text/plain; charset=utf-8')
        self.end_headers()
        self.wfile.write(b"OK\n")

    def handle_get_status(self):
        id, seen, status = self.server.backend.status()
        self.send_response(200)
        self.send_header('Content-Type', 'text/plain; charset=utf-8')
        self.end_headers()
        self.wfile.write(bytes(status, 'utf-8'))
        self.wfile.write(b"\n")

    def handle_post_status(self):
        parms = self.getparms()
        id = parms.get('id')
        seen = parms.get('seen')
        if seen:
            try:
                seen = int(seen)
            except ValueError:
                raise ClientError("Parameter 'seen' must be numeric")
        else:
            seen = 0
        id, seen, status = self.server.backend.status(id, seen)
        result = json.dumps(dict(id=id, seen=seen, status=status))
        self.send_response(200)
        self.send_header('Content-Type', 'text/json; charset=utf-8')
        self.end_headers()
        self.wfile.write(bytes(result, 'utf-8'))
        self.wfile.write(b"\n")

import sys
from urlparse import urlparse
from DIRAC import gLogger
from DIRAC.ConfigurationSystem.Client.ConfigurationData import gConfigurationData
gConfigurationData.setOptionInCFG( '/DIRAC/Security/UseServerCertificate', 'true' )

gLogger.setLevel('FATAL')
from DIRAC.Core.DISET.RPCClient import RPCClient

import SimpleHTTPServer
from BaseHTTPServer import HTTPServer, BaseHTTPRequestHandler
import SocketServer

class MyRequestHandler(BaseHTTPRequestHandler):
  def do_GET(self):
    if self.path == '/self':
      self.send_response(200)
      self.send_header('Content-type','text/html')
      self.end_headers()
      # Send the html message
      self.wfile.write("I am good thanks")
      return

    if not self.path.startswith('/ping'):
      self.send_error(404)
      return

    outputText = "Nothing to report"
    query = urlparse(self.path).query
    queryParams = dict(t.split('=') for t in query.split('&')) if query else {}

    for param in ['host', 'port', 'service']:
      if param not in queryParams:
        self.send_error(400, "Missing Param ! %s"%param )
        return

    rpc = RPCClient("dips://%(host)s:%(port)s/%(service)s"%queryParams)
    res = rpc.ping()
    if not res['OK']:
      self.send_error(418, res['Message'])
      return

    self.send_response(200)
    pingResult = res['Value']


    self.send_header('Content-type','text/html')
    self.end_headers()
    # Send the html message
    self.wfile.write(pingResult)
    return

server = HTTPServer(('0.0.0.0', 1234), MyRequestHandler)

server.serve_forever()


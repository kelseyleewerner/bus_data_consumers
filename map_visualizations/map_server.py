import http.server
import socketserver


# Basic Python SimpleHTTPServer to serve the index.html file that displays a map visualization
#   This server is copied with few modifications from an example


PORT = 80
Handler = http.server.SimpleHTTPRequestHandler

with socketserver.TCPServer(('', PORT), Handler) as http:
  print('Serving at port', PORT)
  http.serve_forever()


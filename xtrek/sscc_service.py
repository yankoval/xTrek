"""HTTPS entry point for the local extension-3 SSCC ledger."""
import os
import ssl
from http.server import ThreadingHTTPServer

from .sscc_local import Ledger, create_handler


def main():
    bind = os.environ['XTREK_SSCC_BIND']
    port = int(os.environ.get('XTREK_SSCC_PORT', '9443'))
    ledger = Ledger(os.environ['XTREK_SSCC_DB'], os.environ['XTREK_SSCC_PREFIX'], 3)
    allowed = {item.strip() for item in os.environ['XTREK_SSCC_ALLOWED_SUBJECTS'].split(';')
               if item.strip()}
    if not allowed:
        raise RuntimeError('at least one client certificate subject must be allowed')
    server = ThreadingHTTPServer((bind, port), create_handler(ledger, allowed))
    context = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
    context.minimum_version = ssl.TLSVersion.TLSv1_2
    context.load_cert_chain(os.environ['XTREK_SSCC_SERVER_CERT'],
                            os.environ['XTREK_SSCC_SERVER_KEY'])
    context.load_verify_locations(cafile=os.environ['XTREK_SSCC_CLIENT_CA'])
    context.verify_mode = ssl.CERT_REQUIRED
    server.socket = context.wrap_socket(server.socket, server_side=True)
    server.serve_forever()


if __name__ == '__main__':
    main()

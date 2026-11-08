"""Cost tests never contact production, cloud metadata or a real broker."""
import socket

import pytest

from .meter import NetworkAccessForbidden


@pytest.fixture(autouse=True)
def no_network(monkeypatch):
    def forbidden(*args, **kwargs):
        raise NetworkAccessForbidden('Cost tests must not open a network connection')

    monkeypatch.setattr(socket.socket, 'connect', forbidden)
    monkeypatch.setattr(socket.socket, 'connect_ex', forbidden)
    monkeypatch.setattr(socket, 'create_connection', forbidden)
    monkeypatch.setenv('AWS_EC2_METADATA_DISABLED', 'true')

import socket

def is_valid_hostname(hostname):
    """
    Check if value is a valid hostname

    :param hostname: Hostname to validate
    :return (bool): True if the hostname is valid, False otherwise
    """
    try:
        socket.getaddrinfo(hostname, None)
        return True
    except socket.gaierror:
        return False


def is_valid_port(port):
    """
    Check if value is a valid port

    :param port: the port number to validate
    :return (bool): True if the port is valid, False otherwise
    """
    try:
        port = int(port)
    except ValueError:
        return False
    return 0 < port <= 65535

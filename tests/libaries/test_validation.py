import pytest
from libraries.validation import is_valid_hostname, is_valid_port

class TestValidation:
    @pytest.mark.parametrize("hostname, expected", [
        ("example.com", True),
        ("localhost", True),
        ("Not valid host", False),
        ("256.256.256.256", False)
    ])
    def test_valid_hostname(self, hostname, expected):
        assert is_valid_hostname(hostname) == expected

    @pytest.mark.parametrize("port, expected", [
        ("8080", True),
        ("12345", True),
        ("not a valid port", False),
        ("-1", False),
        ("70000", False),
        ("abc", False),
        ("0", False),
        ("65535", True),
        ("65536", False),
    ])
    def test_valid_port(self, port, expected):
        assert is_valid_port(port) == expected

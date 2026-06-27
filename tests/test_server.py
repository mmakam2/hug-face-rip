from app.main import server_host_port


def test_default_binds_all_interfaces():
    assert server_host_port({}) == ("0.0.0.0", 8000)


def test_env_overrides_host_and_port():
    assert server_host_port({"HOST": "127.0.0.1", "PORT": "9000"}) == ("127.0.0.1", 9000)


def test_blank_env_values_fall_back_to_defaults():
    assert server_host_port({"HOST": "", "PORT": ""}) == ("0.0.0.0", 8000)

import os

import clickhouse_connect


def get_client():
    return clickhouse_connect.get_client(
        host=os.getenv("CLICKHOUSE_HOST", "clickhouse_server"),
        port=int(os.getenv("CLICKHOUSE_PORT", "8123")),
        username=os.getenv("CLICKHOUSE_USER", "myuser"),
        password=os.getenv("CLICKHOUSE_PASSWORD", "mypassword"),
    )


DATABASE = os.getenv("CLICKHOUSE_DB", "mydb")

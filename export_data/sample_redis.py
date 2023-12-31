import redis


def create_connection(db: int) -> redis.StrictRedis:
    """Initialize redis connection
    :param db: selected db
    :return: connection pool
    """
    pool = redis.ConnectionPool(
        host="localhost",
        port=63791,
        db=db,
        username="aiml",
        password="UJ9Gbhh9uEJTpAZf",
    )
    conn = redis.StrictRedis(
        connection_pool=pool,
        retry_on_timeout=True,
        socket_timeout=10_0000,
        socket_connect_timeout=10_0000,
    )
    return conn


def scan_db_0():
    conn = create_connection(db=0)
    cnt = 0
    for key in conn.scan_iter(match="*"):
        if cnt == 10:
            break
        resp = {
            k.decode("utf-8"): v.decode("utf-8") for k, v in conn.hgetall(key).items()
        }
        print(resp)
        cnt += 1

    # close connection
    conn.close()


def scan_db_1():
    conn = create_connection(db=1)
    cnt = 1
    for key in conn.scan_iter(match="*"):
        if cnt == 10:
            break

        value = conn.get(key).decode("utf-8")
        print(value)

        cnt += 1

    # close connection
    conn.close()


def scan_db_2():
    conn = create_connection(db=2)
    cnt = 1
    for key in conn.scan_iter(match="*"):
        if cnt == 10:
            break

        value = conn.get(key).decode("utf-8")
        print(value)

        cnt += 1

    # close connection
    conn.close()



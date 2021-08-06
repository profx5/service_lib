import asyncio

import aiokafka

retryable_exceptions = [
    ConnectionError,
    asyncio.TimeoutError,
    aiokafka.errors.ProducerClosed,
    aiokafka.errors.ProducerFenced,
    aiokafka.errors.ConcurrentTransactions,
]

try:
    import aiohttp.client_exceptions

    retryable_exceptions.append(aiohttp.ServerTimeoutError)
except ImportError:
    pass

try:
    import aioredis

    retryable_exceptions += [
        aioredis.MaxClientsError,
        aioredis.ChannelClosedError,
        aioredis.ConnectionClosedError,
        aioredis.ConnectionForcedCloseError,
        aioredis.PoolClosedError,
    ]
except ImportError:
    pass

try:
    import pymongo

    retryable_exceptions += [
        pymongo.errors.NetworkTimeout,
        pymongo.errors.NotMasterError,
        pymongo.errors.CursorNotFound,
        pymongo.errors.ExecutionTimeout,
        pymongo.errors.WTimeoutError,
    ]
except ImportError:
    pass

try:
    import psycopg2

    retryable_exceptions += [psycopg2.OperationalError]
except ImportError:
    pass

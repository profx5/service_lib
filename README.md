# service-lib: библиотека для сервисов бекенда


#### Extras:

-   redis: для `RedisComponent`
-   postgres: для `AIOPostgresComponent`
-   kafka: для `kafka_agent.Agent` и `KafkaProducerComponent`
-   web: ставит `FastAPI` и `uvicorn`
-   mongo: ставит `motor`
-   aiohttp: ставит `aiohttp`
-   all: все вместе

Установка extra зависимостей

Poetry

```
poetry add service-lib -E redis
```

pip

```
pip install service-lib[redis]
```

#

### Описание

#### Управлять стейтом приложения

FastAPI

```
     Uvicorn
        |
        | ASGI
        V
     FastAPI
        |
        | startup/shutdown
        V
   StateManager
        |
  state | startup/shutdown
        V
  List[Component]
```

Worker

```
     Worker
       |
       | startup/run/shutdown
       V
     System
       |
       | startup/shutdown
       V
  StateManager
       |
 state | startup/shutdown
       V
 List[Component]
```

_State_ – контейнер состояния приложения

_Component_ – нечто требующее startup/shutdown и/или доступ к State

_StateManager_ – запускает/останавливает компоненты и передает в них State

_System_ – обработчик каких-либо событий (что-то на уровне FastAPI, что будет тригерить какие-то действия)

_Worker_ – стартует/запускает систему, реализует graceful shutdown процесса

### Components

#### AIOPostgresComponent

Обвзяка над [aiopg](https://aiopg.readthedocs.io/en/stable/), требует установки extra `postgres`

#### RedisComponent

Обвзяка над [aioredis](https://aioredis.readthedocs.io/en/v1.3.0/), требует установки extra `redis`

#### KafkaProducerComponent

Обвзяка над продьюсером [aiokafka](https://aiokafka.readthedocs.io/en/stable/producer.html), требует установки extra `kafka`

#### MotorComponent

Обвзяка над [motor](https://motor.readthedocs.io/en/stable/), требует установки extra `mongo`

#### AiohttpSessionComponent

Обвзяка над [aiohttp.ClientSession](https://docs.aiohttp.org/en/stable/client_reference.html), требует установки extra `aiohttp`

### Systems

#### KafkaAgent

Обработчик сообщений из топиков кафки на [aiokafka](https://aiokafka.readthedocs.io/en/stable/consumer.html), требует установки extra `kafka`
[Подробнее](./docs/kafka_agent.md)

### Other

#### Logging

`service_lib.logging.setup_logging`

Инициализация [loguru](https://github.com/Delgan/loguru) и перехвата стандартного логирования

#### Settings

`service_lib.settings.create_settings_class`

Создание pydantic.BaseSettings из описаний компонентов

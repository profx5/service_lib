from service_lib.types import BaseError


class AgentError(BaseError):
    pass


class AgentAlreadyStarted(AgentError):
    code = "agent_already_started"
    message = "Agent already started"


class AlreadySubscribed(AgentError):
    code = "already_subscribed"

    def __init__(self, topic_name: str) -> None:
        self.message = f"Already subscribed to topic {topic_name}"
        super().__init__()


class UnknownTopic(AgentError):
    code = "unknown_topic"

    def __init__(self, topic_name: str) -> None:
        self.message = f"Topic {topic_name} not presented in any subscritpion"


class ParseKeyValueError(AgentError):
    code = "parse_key_value_error"
    message = "Error while parsing key or value of record"

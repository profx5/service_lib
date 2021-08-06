from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from service_lib.kafka_agent.agent import Agent

import aiokafka


class AgentRebalanceListener(aiokafka.ConsumerRebalanceListener):  # type: ignore
    def __init__(self, agent: "Agent") -> None:
        self.agent = agent

    def on_partitions_revoked(self, revoked: list[aiokafka.TopicPartition]) -> None:
        for partition in revoked:
            self.agent.remove_partition_consumer(partition)

    def on_partitions_assigned(self, assigned: list[aiokafka.TopicPartition]) -> None:
        for partition in assigned:
            self.agent.add_partition_consumer(partition)

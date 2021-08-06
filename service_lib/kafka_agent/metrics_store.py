from prometheus_client import Counter, Histogram


class MetricsStore:
    def __init__(self) -> None:
        self.process_message_hist = Histogram(
            "process_message_time_seconds", "Time for message processing", labelnames=("topic",)
        )
        self.errors_counter = Counter(
            "process_message_errors",
            "Count of message processing errors",
            labelnames=("topic", "error_class"),
        )
        self.partition_consumer_created = Counter("partition_consumer_created", "Count of created partition consumers")
        self.partition_consumer_removed = Counter("partition_consumer_removed", "Count of removed partition consumers")


metrics_store = MetricsStore()

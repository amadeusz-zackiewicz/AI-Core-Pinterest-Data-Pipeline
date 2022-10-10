from kafka import KafkaAdminClient
from kafka.admin import NewTopic
from kafka.cluster import ClusterMetadata

class AdminClient(KafkaAdminClient):
    pass


admin = AdminClient(bootstrap_servers="localhost:9092", client_id="Kafka Administrator")

try:
    topics = []
    topics.append(NewTopic(name="Pinterest", num_partitions=3, replication_factor=1))

    admin.create_topics(new_topics=topics)
except:
    pass

try:
    print(admin.describe_topics(topics=["Pinterest"]))
except:
    pass
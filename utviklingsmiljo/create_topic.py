from kafka.admin import KafkaAdminClient, NewTopic

client = KafkaAdminClient(bootstrap_servers='localhost:9092')
topic = [NewTopic(name="test", num_partitions=3, replication_factor=1)]
rsp = client.create_topics(topic)
print(rsp)
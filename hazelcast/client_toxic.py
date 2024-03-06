import hazelcast


def message_listener(event):
    print(f"Received: {event.message}")


def task4():
    hz = hazelcast.HazelcastClient(cluster_name="dev",cluster_members=[])
    try:
        topic = hz.get_topic("my-distributed-topic")
        registration_id = topic.add_listener(message_listener)
        input("Press Enter to exit...\n")
        topic.remove_listener(registration_id)
    finally:
        hz.shutdown()


if __name__ == "__main__":
    task4()

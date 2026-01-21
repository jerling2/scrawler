from confluent_kafka.admin import NewTopic


def get_topic_hse1():
    return NewTopic(
        topic="extract.handshake.job.stage1.v1",
        num_partitions=3,
        replication_factor=1,
        config={
            'cleanup.policy': 'delete',
            'retention.ms': str(24 * 60 * 60 * 1000), # 24 hours
            'segment.ms': str(12 * 60 * 60 * 1000)    # 12 hours
        }
    )

def get_topic_hse2():
    return NewTopic(
        topic="extract.handshake.job.stage2.v1",
        num_partitions=3,
        replication_factor=1,
        config={
            'cleanup.policy': 'delete',
            'retention.ms': str(24 * 60 * 60 * 1000), # 24 hours
            'segment.ms': str(12 * 60 * 60 * 1000)    # 12 hours
        }
    )

def get_topic_hst1():
    return NewTopic(
        topic="raw.handshake.job.stage1.v1",
        num_partitions=3,
        replication_factor=1,
        config={
            'cleanup.policy': 'delete',
            'retention.ms': str(24 * 60 * 60 * 1000), # 24 hours
            'segment.ms': str(12 * 60 * 60 * 1000)    # 12 hours
        }
    )

def get_topic_hst2():
    return NewTopic(
        topic="raw.handshake.job.stage2.v1",
        num_partitions=3,
        replication_factor=1,
        config={
            'cleanup.policy': 'delete',
            'retention.ms': str(24 * 60 * 60 * 1000), # 24 hours
            'segment.ms': str(12 * 60 * 60 * 1000)    # 12 hours
        }
    )

def get_topic_hsl():
    return NewTopic(
        topic="load.handshake.job.v1",
        num_partitions=3,
        replication_factor=1,
        config={
            'cleanup.policy': 'delete',
            'retention.ms': str(24 * 60 * 60 * 1000), # 24 hours
            'segment.ms': str(12 * 60 * 60 * 1000)    # 12 hours
        }
    )

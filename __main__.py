from source import MainControlProgram, MCPHandshakeETLModel, HandshakeExtractor1Codec, InterProcessGateway, KafkaConnectionConfig, KafkaProducerConfig

SEND_E1_CMD = False
DEV_BROKER = InterProcessGateway(KafkaConnectionConfig(producer_config=KafkaProducerConfig.from_env()))
E1_MSG = HandshakeExtractor1Codec(start_page=1, end_page=5, per_page=50)

if __name__ == "__main__":
    if SEND_E1_CMD:
        # For development, manually set to SEND_E1_CMD to True to kickoff the etl pipeline.
        DEV_BROKER.send(HandshakeExtractor1Codec, HandshakeExtractor1Codec.TOPIC, E1_MSG)
        DEV_BROKER.flush(timeout=10)

    mcp = MainControlProgram(MCPHandshakeETLModel())
    mcp.run()

"""
--- E1_MSG (aka Extractor 1 Message)
The command that kicksoff the ETL pipeline.

Example:
>> start_page = 1
>> end_page = 5
>> per_page = 50 (this is the max per_page that HS allows)
number of scrapped pages ~ 500

Note: The ETL pipeline is not 100% efficient. The true number of scrapped pages
will most likely be less than the expected amount. Adding DLQ (dead letter queues)
and logging to better monitor the inefficiencies is on the todo list.

Tip: Use an UI (e.g. Kafbat) to manually change the message offsets, thereby
removing the need to populate the ETL pipeline with new start messages.
This manual process leverages Kafka to send the same message multiple times
through the pipeline. That's why the SEND_E1_CMD variable is set to False.

--- Running the Model
The MCP will run forever unless interrupted by SIGINT or SIGTERM.
Warning: Extractor 2 takes a long time to gracefully shut down.
HSE2 is designed to empty its buffer, that holds 50 target urls, before 
it is allowed to shutdown. This optimization traded shutdown speed for
runtime speed. Speeding up preemption for HSE2 is on the todo list.
"""
from source import (
    MainControlProgram,
    MCPHandshakeETLModel,
    HandshakeExtractor1Codec,
    InterProcessGateway,
    KafkaConnectionConfig, 
    KafkaProducerConfig
)


SEND_E1_CMD = False
DEV_BROKER = InterProcessGateway(KafkaConnectionConfig(producer_config=KafkaProducerConfig.from_env()))
E1_MSG = HandshakeExtractor1Codec(start_page=1, end_page=5, per_page=50)


if __name__ == "__main__":

    if SEND_E1_CMD:
        # For development, manually set SEND_E1_CMD to kickoff the etl pipeline.
        DEV_BROKER.send(HandshakeExtractor1Codec, HandshakeExtractor1Codec.TOPIC, E1_MSG)
        DEV_BROKER.flush(timeout=10)

    mcp = MainControlProgram(MCPHandshakeETLModel())
    mcp.run()

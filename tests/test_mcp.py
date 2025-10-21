import signal
import os
import pytest
from source import (
    MainControlProgram, 
    MCPScrawlerModel
)

@pytest.fixture()
def mcp():
    return MainControlProgram(MCPScrawlerModel())

@pytest.fixture
def signal_alarm_handler():
    pid = os.getpid()

    def timeout_handler(signum, frame):
        _ = signum, frame
        os.kill(pid, signal.SIGINT)

    return timeout_handler

def test_run_until_interrupt(mcp, signal_alarm_handler):
    seconds_until_preempt = 5
    original_handler = signal.signal(signal.SIGALRM, signal_alarm_handler)
    signal.alarm(seconds_until_preempt)
    try:
        mcp.run() #< should run forever until interrupted by user
    except:
        raise
    finally:
        signal.alarm(0)
        signal.signal(signal.SIGALRM, original_handler)
    
import signal
import os
import pytest
from source import (
    MainControlProgram, 
    MCPScrawlerModel,
    MCPHandshakeExtractor1Model,
    MCPHandshakeTransformer1Model,
    MCPHandshakeExtractor2Model,
    MCPHandshakeTransformer2Model,
    MCPHandshakeETLModel
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
    
def test_mcp_hse1_model(signal_alarm_handler):
    UNTIL_PREEMPT = 5
    original_handler = signal.signal(signal.SIGALRM, signal_alarm_handler)
    mcp = MainControlProgram(MCPHandshakeExtractor1Model())
    signal.alarm(UNTIL_PREEMPT)
    try:
        mcp.run()
    except:
        raise
    finally:
        signal.alarm(0)
        signal.signal(signal.SIGALRM, original_handler)

def test_mcp_hst1_model(signal_alarm_handler):
    UNTIL_PREEMPT = 30
    original_handler = signal.signal(signal.SIGALRM, signal_alarm_handler)
    mcp = MainControlProgram(MCPHandshakeTransformer1Model())
    signal.alarm(UNTIL_PREEMPT)
    try:
        mcp.run()
    except:
        raise
    finally:
        signal.alarm(0)
        signal.signal(signal.SIGALRM, original_handler)

def test_mcp_hse2_model(signal_alarm_handler):
    UNTIL_PREEMPT = 30
    original_handler = signal.signal(signal.SIGALRM, signal_alarm_handler)
    mcp = MainControlProgram(MCPHandshakeExtractor2Model())
    signal.alarm(UNTIL_PREEMPT)
    try:
        mcp.run()
    except:
        raise
    finally:
        signal.alarm(0)
        signal.signal(signal.SIGALRM, original_handler)

def test_mcp_hst2_model(signal_alarm_handler):
    UNTIL_PREEMPT = 30
    original_handler = signal.signal(signal.SIGALRM, signal_alarm_handler)
    mcp = MainControlProgram(MCPHandshakeTransformer2Model())
    signal.alarm(UNTIL_PREEMPT)
    try:
        mcp.run()
    except:
        raise
    finally:
        signal.alarm(0)
        signal.signal(signal.SIGALRM, original_handler)

def test_mcp_etl_model(signal_alarm_handler):
    UNTIL_PREEMPT = 60
    original_handler = signal.signal(signal.SIGALRM, signal_alarm_handler)
    mcp = MainControlProgram(MCPHandshakeETLModel())
    signal.alarm(UNTIL_PREEMPT)
    try:
        mcp.run()
    except:
        raise
    finally:
        signal.alarm(0)
        signal.signal(signal.SIGALRM, original_handler)
"""Real dispatch and processes: approval must precede effects and never cover hidden state."""
import json
import threading
import time
from types import SimpleNamespace

import pytest

from tests.tools._child_env_fixtures import child_env, RunToCompletionEnv
from tests.tools.test_cron_smart import homes
from hermes_constants import set_hermes_home_override, reset_hermes_home_override


@pytest.mark.parametrize("transport", ["local", "ssh", "terminal"])
def test_cron_smart_real_execution_is_one_shot(child_env, homes, monkeypatch, transport):
    import model_tools
    from tools import code_execution_tool as code_tool
    from tools import terminal_tool

    verdict = "DENY"
    assessed = []
    def provider(**kwargs):
        assessed.append(kwargs["messages"][1]["content"])
        return SimpleNamespace(choices=[SimpleNamespace(message=SimpleNamespace(content=verdict))])

    monkeypatch.setattr("agent.auxiliary_client.call_llm", provider)
    monkeypatch.setattr(code_tool, "_load_config", lambda: {"mode": "project"})
    if transport == "ssh":
        env = RunToCompletionEnv(child_env)
        monkeypatch.setattr(terminal_tool, "_get_env_config", lambda: {"env_type": "ssh"})
        monkeypatch.setattr(code_tool, "_get_or_create_env", lambda _: (env, "ssh"))
    token = set_hermes_home_override(homes[0])
    witness = child_env / "executed.txt"
    source = f"from pathlib import Path\nPath({str(witness)!r}).write_text('approved')\nprevious_cell = 123\nprint('done')"
    def execute(code):
        if transport == "terminal":
            import shlex
            import sys
            return json.loads(model_tools.handle_function_call(
                "terminal", {"command": shlex.join([sys.executable, "-c", code])}, task_id="cron-smart-test"))
        return json.loads(model_tools.handle_function_call("execute_code", {"code": code}, task_id="cron-smart-test"))
    try:
        for verdict in ("DENY", "ESCALATE", "APPROVE"):
            result = execute(source)
            assert witness.exists() is (verdict == "APPROVE"), result
            if verdict == "APPROVE":
                assert result.get("status") == "success" or result.get("exit_code") == 0, result
                assert witness.read_text() == "approved"
        if transport == "terminal":
            assert len(assessed) == 3
            return
        verdict = "APPROVE"
        result = execute("print('previous_cell' in globals())")
        assert result["status"] == "success", result
        assert result["output"].strip() == "False"
        assert len(assessed) == 4
        assert source in assessed[0]
        if transport == "local":
            from pathlib import Path
            result = execute("print('x' * 1_100_000)")
            assert result["status"] == "success", result
            assert Path(result["stdout_spill_path"]).is_file()
            assert "state persists" not in result.get("warning", "")
            assert result["kernel"]["mode"] == "one-shot"
    finally:
        reset_hermes_home_override(token)


def test_cron_smart_deadline_rejects_late_approval(homes, monkeypatch):
    from tools import approval
    entered, release, finished = threading.Event(), threading.Event(), threading.Event()
    def provider(**kwargs):
        entered.set()
        try:
            release.wait(3)
            return SimpleNamespace(choices=[SimpleNamespace(message=SimpleNamespace(content="APPROVE"))])
        finally:
            finished.set()
    monkeypatch.setattr("agent.auxiliary_client.call_llm", provider)
    monkeypatch.setattr("agent.auxiliary_client._get_task_timeout", lambda _: 0.1)
    token = set_hermes_home_override(homes[0])
    try:
        start = time.monotonic()
        result = approval.check_all_command_guards('python -c "print(123)"', "local")
        assert time.monotonic() - start < 2
        assert entered.is_set()
        assert result["approved"] is False
        assert result["approval_failure"] == "timeout"
        release.set()
        assert finished.wait(2)
        assert result["approved"] is False
        assert not approval.is_approved("cron-smart-test", "execute_code")
    finally:
        release.set()
        reset_hermes_home_override(token)

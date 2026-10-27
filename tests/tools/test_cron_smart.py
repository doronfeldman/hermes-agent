"""Cron Smart is one-shot, fails closed, and stays scoped to its profile."""
from types import SimpleNamespace
from unittest.mock import Mock

import pytest
from gateway.session_context import reset_session_vars, set_session_vars
from hermes_constants import set_hermes_home_override, reset_hermes_home_override
from tools import approval, approval_context


@pytest.fixture
def homes(tmp_path, monkeypatch):
    reset_session_vars()
    set_session_vars(cron_session="1", platform="telegram", session_key="cron-smart-test")
    monkeypatch.setenv("HERMES_EXEC_ASK", "1")
    monkeypatch.setenv("HERMES_INTERACTIVE", "1")
    monkeypatch.setattr(approval, "_YOLO_MODE_FROZEN", False)
    approval.clear_session("cron-smart-test")
    paths = []
    for name, mode in (("a", "smart"), ("b", "deny")):
        home = tmp_path / name
        home.mkdir()
        (home / "config.yaml").write_text(
            f"approvals:\n  mode: manual\n  cron_mode: {mode}\n"
            f"  smart_policy: Policy for profile {name}\n  deny: ['*forbidden*']\n"
            "command_allowlist: []\n"
        )
        paths.append(home)
    monkeypatch.setattr(approval, "submit_pending", Mock(side_effect=AssertionError("must not queue")))
    monkeypatch.setattr(approval, "_human_decision", Mock(side_effect=AssertionError("must not prompt")))
    yield paths
    approval.clear_session("cron-smart-test")
    reset_session_vars()


def invoke(kind):
    if kind == "code":
        return approval.check_execute_code_guard("print('safe')", "local")
    if kind == "tool":
        return approval.request_tool_approval("example", "Human must approve the exact operation")
    guard = approval.check_all_command_guards if kind == "combined" else approval.check_dangerous_command
    return guard('python -c "print(123)"', "local")


@pytest.mark.parametrize("kind", ["combined", "dangerous", "code", "tool"])
@pytest.mark.parametrize("answer", ["APPROVE", "DENY", "ESCALATE", "", "nonsense", TimeoutError(), RuntimeError()])
def test_cron_smart_verdicts_are_one_shot_and_never_wait(homes, monkeypatch, kind, answer):
    calls = []
    def provider(**kwargs):
        calls.append(kwargs)
        assert kwargs["task"] == "approval"
        assert kwargs["timeout"] > 0
        assert "Policy for profile a" in kwargs["messages"][0]["content"]
        if isinstance(answer, Exception):
            raise answer
        return SimpleNamespace(choices=[SimpleNamespace(message=SimpleNamespace(content=answer))])
    monkeypatch.setattr("agent.auxiliary_client.call_llm", provider)
    token = set_hermes_home_override(homes[0])
    try:
        assert approval_context._get_cron_approval_mode() == "smart"
        for _ in range(2):
            result = invoke(kind)
            assert result["approved"] is (answer == "APPROVE" and kind != "tool")
            assert not result.get("approval_pending")
            if kind == "tool" or answer == "ESCALATE":
                assert result["status"] == "approval_required"
                assert "interactive" in result["message"].lower()
            elif isinstance(answer, Exception) or answer in ("", "nonsense"):
                assert result["status"] == "approval_error"
                assert "provider" in result["message"].lower()
        assert len(calls) == (0 if kind == "tool" else 2)
    finally:
        reset_hermes_home_override(token)


def test_cron_smart_profile_switching_and_safety_floors(homes, monkeypatch):
    provider = Mock(return_value=SimpleNamespace(choices=[SimpleNamespace(message=SimpleNamespace(content="APPROVE"))]))
    monkeypatch.setattr("agent.auxiliary_client.call_llm", provider)
    for home, expected in ((homes[0], True), (homes[1], False), (homes[0], True)):
        token = set_hermes_home_override(home)
        try:
            assert invoke("combined")["approved"] is expected
            for guard in (approval.check_all_command_guards, approval.check_dangerous_command):
                for command in ("rm -rf /", "echo forbidden"):
                    assert not guard(command, "local")["approved"]
            assert not approval.check_execute_code_guard("print('forbidden')", "local")["approved"]
        finally:
            reset_hermes_home_override(token)
    assert provider.call_count == 2


@pytest.mark.parametrize("kind", ["combined", "dangerous", "code", "tool"])
def test_cron_smart_does_not_reuse_broad_grants(homes, monkeypatch, kind):
    provider = Mock(return_value=SimpleNamespace(choices=[SimpleNamespace(message=SimpleNamespace(content="DENY"))]))
    monkeypatch.setattr("agent.auxiliary_client.call_llm", provider)
    monkeypatch.setattr(approval, "is_approved", lambda *args: True)
    monkeypatch.setattr(approval, "_command_matches_permanent_allowlist", lambda *args: True)
    token = set_hermes_home_override(homes[0])
    try:
        assert not invoke(kind)["approved"]
        assert provider.call_count == (0 if kind == "tool" else 1)
        if kind in {"combined", "dangerous"}:
            guard = approval.check_all_command_guards if kind == "combined" else approval.check_dangerous_command
            assert guard("echo hello", "local")["approved"]

    finally:
        reset_hermes_home_override(token)

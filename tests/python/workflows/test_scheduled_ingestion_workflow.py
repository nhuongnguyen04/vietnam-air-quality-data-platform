from pathlib import Path

import pytest
import yaml

WORKFLOW_PATH = Path(".github/workflows/scheduled_ingestion.yml")


@pytest.mark.unit
def test_scheduled_ingestion_notifies_telegram_on_failure() -> None:
    workflow = yaml.safe_load(WORKFLOW_PATH.read_text())
    steps = workflow["jobs"]["ingest"]["steps"]
    step_names = [step["name"] for step in steps]

    upload_index = step_names.index("Upload to Google Drive")
    notify_index = step_names.index("Notify Telegram on failure")
    notify_step = steps[notify_index]

    assert notify_index > upload_index
    assert notify_step["if"] == "${{ failure() }}"
    assert notify_step["env"]["TELEGRAM_BOT_TOKEN"] == "${{ secrets.TELEGRAM_SYS_BOT_TOKEN }}"
    assert notify_step["env"]["TELEGRAM_CHAT_ID"] == "${{ secrets.TELEGRAM_SYS_CHAT_ID }}"
    assert "https://api.telegram.org/bot${TELEGRAM_BOT_TOKEN}/sendMessage" in notify_step["run"]
    assert "curl --fail" in notify_step["run"]

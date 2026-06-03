"""
Subprocess Runner - Standardized helper to execute python tasks from Airflow DAGs.
"""

import logging
import os
import sys
import subprocess
from typing import List, Dict

logger = logging.getLogger(__name__)

def run_python_job(
    script_path: str,
    args: List[str] = None,
    env_overrides: Dict[str, str] = None,
    cwd: str = None,
    timeout: int = None,
    raise_on_error: bool = True
) -> subprocess.CompletedProcess:
    """
    Standardized subprocess runner for python scripts with proper error handling and logging.
    
    Args:
        script_path: Path to the python script (relative to cwd or absolute).
        args: List of command-line arguments.
        env_overrides: Dictionary of environment variable overrides.
        cwd: Directory where the script should be executed.
        timeout: Execution timeout in seconds.
        raise_on_error: If True, raises an exception on non-zero exit code.
        
    Returns:
        CompletedProcess: The result of the subprocess run.
    """
    if args is None:
        args = []

    # Use the current python interpreter to run the script
    cmd = [sys.executable, script_path] + args
    
    env = os.environ.copy()
    if env_overrides:
        # Convert all keys/values to strings for safety
        env.update({str(k): str(v) for k, v in env_overrides.items() if v is not None})

    print(f"Running command: {' '.join(cmd)}")
    if cwd:
        print(f"Working directory: {cwd}")

    result = subprocess.run(
        cmd,
        cwd=cwd,
        env=env,
        capture_output=True,
        text=True,
        timeout=timeout
    )

    if result.stdout:
        print(f"STDOUT:\n{result.stdout.strip()}")
    if result.stderr:
        print(f"STDERR:\n{result.stderr.strip()}")

    if result.returncode != 0:
        msg = f"Command failed with return code {result.returncode}: {' '.join(cmd)}"
        print(f"Error: {msg}")
        if raise_on_error:
            raise RuntimeError(msg)

    return result

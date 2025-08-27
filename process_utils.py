import os
import time
import subprocess
from typing import Optional


def _parse_ps_etime_to_seconds(etime_str: str) -> Optional[int]:
    """
    Convert ps etime format to seconds.
    etime formats:
      - MM:SS
      - HH:MM:SS
      - d-HH:MM:SS
    Returns None if parsing fails.
    """
    etime_str = (etime_str or "").strip()
    days = 0
    if '-' in etime_str:
        dpart, rest = etime_str.split('-', 1)
        try:
            days = int(dpart)
        except ValueError:
            return None
    else:
        rest = etime_str

    parts = rest.split(':')
    try:
        if len(parts) == 2:
            h, m, s = 0, int(parts[0]), int(parts[1])
        elif len(parts) == 3:
            h, m, s = int(parts[0]), int(parts[1]), int(parts[2])
        else:
            return None
    except ValueError:
        return None

    return days * 86400 + h * 3600 + m * 60 + s


def kill_old_socket_processes(max_age_seconds: int = 2 * 3600, name_filter: str = "fotmob_socket_") -> None:
    """
    Find and kill processes whose command contains `name_filter` and which are
    older than `max_age_seconds` using `ps -axo pid,etime,command`.

    First sends SIGTERM, waits briefly, then SIGKILL for any still alive.
    """
    try:
        ps = subprocess.run([
            "ps", "-axo", "pid,etime,command"
        ], capture_output=True, text=True)
        if ps.returncode != 0:
            print(f"ps command failed: {ps.stderr}")
            return

        lines = ps.stdout.strip().splitlines()
        if lines and lines[0].lower().startswith("pid "):
            lines = lines[1:]

        target_pids = []
        for line in lines:
            parts = line.strip().split(None, 2)
            if len(parts) < 3:
                continue
            pid_str, etime_str, command = parts[0], parts[1], parts[2]
            if name_filter not in command:
                continue
            age = _parse_ps_etime_to_seconds(etime_str)
            if age is None or age < max_age_seconds:
                continue
            target_pids.append(int(pid_str))

        if not target_pids:
            print(f"Cleanup: no old processes (filter='{name_filter}', >= {max_age_seconds}s)")
            return

        # SIGTERM
        for pid in target_pids:
            try:
                os.kill(pid, 15)
            except Exception as e:
                print(f"Failed to SIGTERM PID {pid}: {e}")
        time.sleep(1.0)

        # SIGKILL leftovers
        ps2 = subprocess.run([
            "ps", "-axo", "pid,etime,command"
        ], capture_output=True, text=True)
        if ps2.returncode == 0:
            lines2 = ps2.stdout.strip().splitlines()
            if lines2 and lines2[0].lower().startswith("pid "):
                lines2 = lines2[1:]
            for line in lines2:
                parts = line.strip().split(None, 2)
                if len(parts) < 3:
                    continue
                pid_str, etime_str, command = parts[0], parts[1], parts[2]
                if name_filter not in command:
                    continue
                age = _parse_ps_etime_to_seconds(etime_str)
                if age is None or age < max_age_seconds:
                    continue
                try:
                    os.kill(int(pid_str), 9)
                except Exception as e:
                    print(f"Failed to SIGKILL PID {pid_str}: {e}")

        print(f"Cleanup: processed {len(target_pids)} old processes (filter='{name_filter}', >= {max_age_seconds}s)")
    except Exception as e:
        print(f"Error during process cleanup: {e}")

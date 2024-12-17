import sys
import subprocess


def _check_exit_code(exit_code: int):
    if exit_code != 0:
        sys.exit(exit_code)
    return exit_code


def _run_safely(cmd: list[str], env=None) -> int:
    try:
        process = subprocess.Popen(cmd, env=env, stdin=sys.stdin, stdout=sys.stdout)
        process.communicate()
        return process.returncode
    except KeyboardInterrupt:
        return 0


def test():
    cmd = [
        "pytest",
        "src",
    ]
    return _check_exit_code(_run_safely(cmd))

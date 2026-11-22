import json
import os
from pathlib import Path
import subprocess
import tempfile
import unittest


SCRIPT = Path(__file__).with_name("circus-release-gate.sh")
MAIN = {"context": "circus/jobset/main", "state": "success"}
ARM = {"context": "circus/jobset/main-arm", "state": "success"}
SUCCESS = {"status": "completed", "conclusion": "success"}


class CircusReleaseGateTest(unittest.TestCase):
    def run_gate(self, statuses, runs, event="status"):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            gh = root / "gh"
            gh.write_text('#!/bin/sh\ncase "$2" in */status) printf "%s\\n" "$STATUSES";; *) printf "%s\\n" "$RUNS";; esac\n')
            gh.chmod(0o755)
            output = root / "output"
            env = {**os.environ, "PATH": f"{root}:{os.environ['PATH']}", "EVENT_NAME": event,
                   "GITHUB_REPOSITORY": "owner/repo", "GITHUB_SHA": "a" * 40,
                   "GITHUB_OUTPUT": str(output), "STATUSES": json.dumps({"statuses": statuses}),
                   "RUNS": json.dumps({"workflow_runs": runs})}
            subprocess.run(["bash", str(SCRIPT)], env=env, check=True)
            return output.read_text().strip()

    def test_release_requires_both_jobsets_and_successful_github_ci(self):
        self.assertEqual(self.run_gate([MAIN, ARM], [SUCCESS]), "ready=true")
        self.assertEqual(self.run_gate([MAIN], [SUCCESS]), "ready=false")
        self.assertEqual(self.run_gate([MAIN, ARM | {"state": "pending"}], [SUCCESS]), "ready=false")
        self.assertEqual(self.run_gate([MAIN, ARM], []), "ready=false")
        self.assertEqual(self.run_gate([MAIN, ARM], [SUCCESS | {"conclusion": "failure"}, SUCCESS]), "ready=false")

    def test_manual_dispatch_remains_available(self):
        self.assertEqual(self.run_gate([], [], event="workflow_dispatch"), "ready=true")


if __name__ == "__main__":
    unittest.main()

import json
import os
from pathlib import Path
import subprocess
import tempfile
import unittest

SCRIPT = Path(__file__).with_name("circus-static-release-ready.sh").resolve()


class StaticReleaseGateTest(unittest.TestCase):
    def test_waits_for_cached_commit_then_selects_its_tags(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            subprocess.run(["git", "init", "-q", str(root)], check=True)
            subprocess.run(["git", "-C", str(root), "-c", "user.name=Test", "-c",
                            "user.email=test@example.com", "commit", "--allow-empty",
                            "-qm", "test"], check=True)
            subprocess.run(["git", "-C", str(root), "tag", "v1-pschmitt.1"], check=True)
            revision = subprocess.check_output(["git", "-C", str(root), "rev-parse", "HEAD"], text=True).strip()
            gh = root / "gh"
            gh.write_text('#!/bin/sh\nprintf "%s\\n" "$MOCK_STATUS"\n')
            gh.chmod(0o755)
            output = root / "output"
            env = dict(os.environ, PATH=f"{root}:{os.environ['PATH']}", GITHUB_OUTPUT=str(output),
                       GITHUB_REPOSITORY="owner/repo", GITHUB_SHA=revision, EVENT_NAME="status")
            for state, expected in [("pending", False), ("success", True)]:
                output.write_text("")
                env["MOCK_STATUS"] = json.dumps({"statuses": [{"context": "circus/artifacts", "state": state}]})
                subprocess.run(["bash", str(SCRIPT)], cwd=root, env=env, check=True)
                values = dict(line.split("=", 1) for line in output.read_text().splitlines())
                self.assertEqual(values["ready"], str(expected).lower())
                self.assertEqual(json.loads(values["tags"]), ["v1-pschmitt.1"] if expected else [])


if __name__ == "__main__":
    unittest.main()

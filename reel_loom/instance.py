import fcntl
import json
import os
import uuid
from .errors import LoomError


class InstanceLock:
    def __init__(self, root):
        self.path = root / "reel_loom.lock"
        self.owner_path = root / "reel_loom.owner.json"
        self.token = uuid.uuid4().hex
        self.handle = None

    def owner(self):
        try:
            value = json.loads(self.owner_path.read_text())
            return value if isinstance(value, dict) else {}
        except (OSError, ValueError):
            return {}

    def owner_alive(self, record):
        pid = record.get("pid")
        if not isinstance(pid, int) or pid <= 0:
            return False
        try:
            os.kill(pid, 0)
        except ProcessLookupError:
            return False
        except PermissionError:
            return True
        return True

    def acquire(self):
        record = self.owner()
        if record and not self.owner_alive(record):
            raise LoomError("instance_active", "The media workspace has an unfinished owner record", 409)
        self.handle = self.path.open("a+")
        try:
            fcntl.flock(self.handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            self.close()
            raise LoomError("instance_active", "Another service is using this media workspace", 409) from exc
        self.owner_path.write_text(json.dumps({"pid": os.getpid(), "token": self.token}))
        return self

    def close(self):
        if self.handle is not None:
            fcntl.flock(self.handle, fcntl.LOCK_UN)
            self.handle.close()
            self.handle = None
            self.path.unlink(missing_ok=True)
            self.owner_path.unlink(missing_ok=True)

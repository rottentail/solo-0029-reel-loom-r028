import fcntl
import json
import os
import tempfile
import uuid
from .errors import LoomError


class InstanceLock:
    """Single-owner guard for one media workspace.

    Mutual exclusion is provided exclusively by an ``flock`` held for the whole
    service lifetime. The lock file itself is never unlinked: deleting it while a
    service still holds the inode lock would let a later process lock a fresh
    inode and run against the same workspace. The owner JSON is an advisory
    record written atomically while the lock is held, so operators can see who
    owns the workspace and a crashed process can be distinguished from a live
    one before falling back to the kernel lock.
    """

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
        if record and self.owner_alive(record) and record.get("pid") != os.getpid():
            raise LoomError("instance_active", "Another service is using this media workspace", 409)
        # A dead or unreadable owner record is a leftover from a crash. Do not
        # refuse here: the flock below is authoritative and can only be taken
        # once the previous owner has actually released it (process exit).
        self.handle = self.path.open("a+")
        try:
            fcntl.flock(self.handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            self.release_handle()
            raise LoomError("instance_active", "Another service is using this media workspace", 409) from exc
        self.write_owner({"pid": os.getpid(), "token": self.token})
        return self

    def write_owner(self, record):
        fd, temp = tempfile.mkstemp(prefix=".owner-", dir=self.owner_path.parent)
        try:
            with os.fdopen(fd, "w") as target:
                json.dump(record, target)
                target.flush()
                os.fsync(target.fileno())
            os.replace(temp, self.owner_path)
        except BaseException:
            if os.path.exists(temp):
                os.unlink(temp)
            raise

    def release_handle(self):
        if self.handle is not None:
            try:
                fcntl.flock(self.handle, fcntl.LOCK_UN)
            except OSError:
                pass
            self.handle.close()
            self.handle = None

    def close(self):
        if self.handle is None:
            # Never reached acquire(): do not touch another owner's files.
            return
        record = self.owner()
        self.release_handle()
        # Only remove the advisory owner record when it is still ours. The lock
        # file stays in place so its inode keeps serializing future owners.
        if record.get("token") == self.token:
            self.owner_path.unlink(missing_ok=True)

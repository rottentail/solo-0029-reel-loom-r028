from .database import Database
from .blobs import BlobStore
from .assets import Assets
from .reels import Reels
from .looks import Looks
from .captions import Captions
from .requests import RequestKeys
from .planner import Planner
from .processes import Processes
from .tickets import Tickets
from .artifacts import Artifacts
from .rendering import Renderer
from .analysis import AudioAnalysis
from .frames import Frames
from .imprints import Imprints
from .resources import Resources
from .instance import InstanceLock
from .batches import RenderBatches
from .conformance import Conformance
from .lineage import Lineage


class Application:
    def __init__(self, settings, start_workers=True):
        self.settings = settings.prepare()
        self.lock = InstanceLock(settings.root)
        self.renderer = None
        self.recovery = {"interrupted": 0}
        # Take the workspace lock before touching any persisted state. Recovery
        # rewrites tickets left in 'running', so a process that fails to become
        # the owner must never run it and disturb the service still encoding.
        self.lock.acquire()
        try:
            self.db = Database(settings.root)
            self.blobs = BlobStore(settings.root, self.db)
            self.keys = RequestKeys()
            self.assets = Assets(self.db, self.blobs, settings)
            self.reels = Reels(self.db, self.assets, self.keys, settings)
            self.looks = Looks(self.db)
            self.captions = Captions(self.db, self.assets, self.reels)
            self.planner = Planner(self.assets, self.reels, self.looks)
            self.processes = Processes(settings)
            self.tickets = Tickets(self.db, self.planner, self.keys)
            self.artifacts = Artifacts(self.db, self.blobs)
            self.batches = RenderBatches(self.db, self.planner, self.tickets, self.keys)
            self.conformance = Conformance(settings, self.blobs, self.tickets, self.artifacts)
            self.lineage = Lineage(self.db, self.tickets)
            self.analysis = AudioAnalysis(self.db, self.assets, self.blobs, self.processes)
            self.frames = Frames(self.assets, self.blobs, self.processes, self.artifacts)
            self.imprints = Imprints(self.db, self.blobs, self.tickets, self.captions)
            self.resources = Resources(self.db, self.blobs)
            with self.db.write_unit() as db:
                self.db.event(db, self.lock.token, "startup_opened", {"owner": self.lock.owner()})
            self.recovery = self.tickets.recover(self.lock.token)
            self.tickets.finish_startup(self.lock.token)
            self.renderer = Renderer(settings, self.blobs, self.tickets, self.planner, self.processes, self.artifacts)
            if start_workers:
                self.renderer.start()
        except BaseException as exc:
            if hasattr(self, "db"):
                try:
                    with self.db.write_unit() as db:
                        self.db.event(db, self.lock.token, "startup_refused", {"reason": str(exc)[:300]})
                except BaseException:
                    pass
            self.close()
            raise

    def health(self):
        return {"service": "reel-loom", "version": "0.1.0", "status": "ready",
                **self.db.health(), "blobs": self.blobs.stats(), "recovery": self.recovery,
                "limits": self.settings.public()}

    def close(self):
        if self.renderer is not None:
            self.renderer.close()
            self.renderer = None
        self.lock.close()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()

import threading
import time
from queue import Queue, Empty
from typing import Any, Callable, Dict, Iterable, List, Optional, Set, Tuple


class _Subscriber:
    def __init__(self, topics: Set[str], flt: Optional[Callable[[str, Dict[str, Any]], bool]], maxsize: int = 512):
        self.topics = topics
        self.filter = flt
        self.q: "Queue[Tuple[str, Dict[str, Any]]]" = Queue(maxsize=maxsize)
        self.alive = True

    def put(self, topic: str, payload: Dict[str, Any]) -> None:
        if not self.alive:
            return
        try:
            if self.filter and not self.filter(topic, payload):
                return
            self.q.put_nowait((topic, payload))
        except Exception:
            # Drop on overflow or any queue error
            pass

    def get(self, timeout: Optional[float] = None) -> Optional[Tuple[str, Dict[str, Any]]]:
        if not self.alive:
            return None
        try:
            return self.q.get(timeout=timeout)
        except Empty:
            return None

    def close(self) -> None:
        self.alive = False


class EventBus:
    """Simple in-memory pub/sub for server-originated UI updates.

    - Thread-safe publish/subscribe
    - Best-effort (drops on overflow)
    - Designed for single-process dev or small deployments
    """

    def __init__(self) -> None:
        self._subs: List[_Subscriber] = []
        self._lock = threading.Lock()

    def publish(self, topic: str, payload: Dict[str, Any]) -> None:
        with self._lock:
            subs = list(self._subs)
        for s in subs:
            try:
                if topic in s.topics or "*" in s.topics:
                    s.put(topic, payload)
            except Exception:
                # Ignore subscriber errors
                pass

    def subscribe(
        self,
        topics: Iterable[str],
        flt: Optional[Callable[[str, Dict[str, Any]], bool]] = None,
        maxsize: int = 512,
    ) -> _Subscriber:
        s = _Subscriber(set(topics), flt, maxsize=maxsize)
        with self._lock:
            self._subs.append(s)
        return s

    def unsubscribe(self, s: _Subscriber) -> None:
        try:
            s.close()
        except Exception:
            pass
        with self._lock:
            try:
                self._subs.remove(s)
            except ValueError:
                pass


# Global singleton bus
bus = EventBus()


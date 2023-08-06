import collections
from threading import RLock
from typing import Any, Generic, KeysView, TypeVar, Union

KT = TypeVar("KT")
VT = TypeVar("VT")


class ConcurrentDict(collections.UserDict, Generic[KT, VT]):
    def __init__(self) -> None:
        super().__init__()
        self._lock = RLock()

    def __getitem__(self, key: KT) -> Any:
        with self._lock:
            return super().__getitem__(key)

    def __setitem__(self, key: KT, value: VT) -> None:
        with self._lock:
            return super().__setitem__(key, value)

    def get(self, key: Any, default: Union[Any, VT] = None) -> Union[Any, VT]:
        with self._lock:
            return super().get(key, default)

    def clear(self) -> None:
        with self._lock:
            super().clear()

    def __delitem__(self, key: KT) -> None:
        with self._lock:
            super().__delitem__(key)

    def keys(self) -> KeysView[KT]:
        with self._lock:
            return super().keys()

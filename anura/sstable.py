import os
from bisect import bisect
from datetime import datetime
from gzip import compress, decompress
from itertools import zip_longest
from pathlib import Path
from typing import Any, Generic, Iterator, List, Optional, Sequence, Tuple

from anura.algorithms import chunk
from anura.constants import BLOCK_SIZE, SPARSE_IDX_EXT, SSTABLE_EXT, TMP_EXT
from anura.io import decode, encode, read_block, write_from
from anura.metadata import TableMetadata
from anura.model import K, MemNode, V
from anura.types import LongType


def rename_tmp_table(tmp_path: Path) -> Path:
    new_path = Path(os.path.splitext(tmp_path)[0])
    os.rename(tmp_path, new_path)
    return new_path


class SSTable(Generic[K, V]):
    _offset_meta = LongType()

    def __init__(self, path: Path, metadata: TableMetadata, serial: Optional[int] = None, is_temp: bool = False):
        self._index: List[Tuple[K, int]] = []
        self._metadata = list(metadata)
        self._index_meta = (metadata.key_type, self._offset_meta)
        # TODO microsecond precision
        self.serial = serial or int(datetime.utcnow().timestamp())
        self._is_temp = is_temp
        tmp_ext = TMP_EXT if self._is_temp else ""
        self._table_path = path / f"{self.serial}.{SSTABLE_EXT}{tmp_ext}"
        self._index_path = path / f"{self.serial}.{SPARSE_IDX_EXT}{tmp_ext}"

    @property
    def is_temp(self) -> bool:
        return self._is_temp

    def commit(self) -> None:
        if not self._is_temp:
            raise ValueError("SSTable is already effective")

        self._table_path = rename_tmp_table(self._table_path)
        self._index_path = rename_tmp_table(self._index_path)
        self._is_temp = False

    @staticmethod
    def _search(key: K, block: Sequence[Any]) -> Optional[MemNode[K, V]]:
        j = bisect(block, key, key=lambda x: x[0]) - 1  # type: ignore[call-overload]
        if j >= 0 and key == block[j][0]:
            return MemNode[K, V](*block[j])
        return None

    def write(self, it: Iterator, block_size: int = BLOCK_SIZE) -> None:
        # TODO consider using mmap
        pipeline = self._write_pipeline(it, block_size)
        write_from(self._table_path, pipeline)

        it = encode(self._index, self._index_meta)
        write_from(self._index_path, it)

    def _write_pipeline(self, it: Iterator, block_size: int) -> Iterator[bytes]:
        offset = 0
        for block in chunk(it, block_size):
            self._index.append((block[0].key, offset))
            acc = b"".join(encode(block, self._metadata))
            raw = compress(acc)
            yield raw
            offset += len(raw)

    def find(self, key: K) -> Optional[MemNode[K, V]]:
        i = bisect(self._index, key, key=lambda x: x[0])  # type: ignore[call-overload]
        if i == 0:
            return None

        offset = self._index[i - 1][1]
        length = -1
        if i < len(self._index):
            length = self._index[i][1] - offset

        # read pipeline
        block = self._read_pipeline(offset, length)

        # search in block
        return self._search(key, list(block))

    def _read_pipeline(self, offset: int, length: int) -> Iterator[Sequence[Any]]:
        raw = read_block(self._table_path, offset=offset, n=length)
        uncompressed = decompress(raw)
        yield from decode(uncompressed, self._metadata)

    def seq_scan(self) -> Iterator[MemNode[K, V]]:
        for (_, offset), _next in zip_longest(self._index, self._index[1:]):
            length = -1
            if _next:
                length = _next[1] - offset

            for record in self._read_pipeline(offset, length):
                yield MemNode[K, V](*record)

    def __iter__(self) -> Iterator[MemNode[K, V]]:
        return self.seq_scan()

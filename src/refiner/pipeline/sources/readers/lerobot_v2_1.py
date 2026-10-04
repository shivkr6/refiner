from __future__ import annotations

from collections.abc import Iterator, Mapping, Sequence
from typing import Any
from fsspec.spec import AbstractFileSystem

from refiner.io.datafolder import DataFolderLike
from refiner.io.fileset import DataFileSet
from refiner.pipeline.data.shard import Shard
from refiner.pipeline.sources.base import BaseSource, SourceUnit
from refiner.pipeline.sources.readers.utils import DEFAULT_TARGET_SHARD_BYTES


class LeRobotV2_1EpisodeReader(BaseSource):
    """Read LeRobot v2.1 episode datasets."""

    name = "read_lerobot_v2_1"

    def __init__(
        self,
        inputs: DataFolderLike | Sequence[DataFolderLike],
        *,
        fs: AbstractFileSystem | None = None,
        storage_options: Mapping[str, Any] | None = None,
        target_shard_bytes: int = DEFAULT_TARGET_SHARD_BYTES,
        num_shards: int | None = None,
        skip_malformed_rows: bool = False,
    ) -> None:
        self._root_fileset = DataFileSet.resolve(
            inputs,
            fs=fs,
            storage_options=storage_options,
            expect_type="folder",
        )
        if not self._root_fileset.entries:
            raise ValueError("LeRobot v2.1 reader requires at least one dataset root")

        self.target_shard_bytes = target_shard_bytes
        self.num_shards = num_shards
        self.skip_malformed_rows = skip_malformed_rows

    def _io_refiner_extras(self) -> tuple[str, ...]:
        return self._root_fileset.required_refiner_extras()

    def list_shards(self) -> list[Shard]:
        # TODO: Read meta/episodes.jsonl and create Shards for each episode (or batches)
        raise NotImplementedError("list_shards not yet implemented for v2.1")

    def read_shard(self, shard: Shard) -> Iterator[SourceUnit]:
        # TODO: Given a Shard, read the specific episode parquet files and yield LeRobotTabular
        raise NotImplementedError("read_shard not yet implemented for v2.1")

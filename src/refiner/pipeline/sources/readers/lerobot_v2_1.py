from __future__ import annotations

from collections.abc import Iterator, Mapping, Sequence
from typing import Any
from fsspec.spec import AbstractFileSystem

from refiner.io import DataFolder
from refiner.io.datafolder import DataFolderLike
from refiner.io.fileset import DataFileSet
from refiner.pipeline.data.shard import Shard
from refiner.pipeline.sources.base import BaseSource, SourceUnit
from refiner.pipeline.sources.readers.utils import DEFAULT_TARGET_SHARD_BYTES
import json
from functools import cached_property
from refiner.robotics.lerobot_format import (
    LeRobotInfo,
    LeRobotTasks,
    LeRobotStatsFile,
    LeRobotMetadata,
)
from refiner.robotics.lerobot_format.metadata.metadata import merge_metadata

_DEFAULT_EPISODES_GLOB_ROOT = "meta/episodes.jsonl"
_INFO_JSON = "meta/info.json"
_STATS_JSONL = "meta/episodes_stats.jsonl"
_TASKS_JSONL = "meta/tasks.jsonl"


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

        self._roots: tuple[DataFolder, ...] | None = None
        self._episode_reader = None

    @property
    def roots(self) -> tuple[DataFolder, ...]:
        if self._roots is None:
            roots = self._root_fileset.datafolders
            if not roots:
                raise ValueError(
                    "LeRobot v2.1 reader requires at least one dataset root"
                )
            self._roots = roots
        return self._roots

    def _json_reader(self):
        if self._episode_reader is None:
            from refiner.pipeline.sources.readers.json import JsonReader

            self._episode_reader = JsonReader(
                inputs=tuple(
                    (str(root.file(_DEFAULT_EPISODES_GLOB_ROOT).abs_path()), root.fs)
                    for root in self.roots
                ),
                recursive=False,
                target_shard_bytes=self.target_shard_bytes,
                num_shards=self.num_shards,
                file_path_column=None,
                lines=True,
            )
        return self._episode_reader

    def _io_refiner_extras(self) -> tuple[str, ...]:
        return self._root_fileset.required_refiner_extras()

    @cached_property
    def _metadata_bundle(
        self,
    ) -> tuple[tuple[LeRobotMetadata, ...], tuple[dict[int, int], ...]]:
        """Load and merge metadata for all dataset roots once per reader."""
        loaded = [self._load_metadata(root) for root in self.roots]
        return merge_metadata(loaded)

    def _load_metadata(self, root: DataFolder) -> LeRobotMetadata:
        """Load one dataset root's info, tasks, and episode-level stats files."""
        # 1. Load info
        with root.file(_INFO_JSON).open("rb") as handle:
            info = LeRobotInfo.from_json_dict(json.loads(handle.read()))

        # 2. Load tasks
        with root.file(_TASKS_JSONL).open("rb") as handle:
            task_rows = [json.loads(line) for line in handle]
            tasks = LeRobotTasks.from_rows(task_rows)

        # 3. Load and aggregate stats
        episode_stats: list[LeRobotStatsFile] = []
        with root.file(_STATS_JSONL).open("rb") as handle:
            for line in handle:
                record = json.loads(line)
                episode_stats.append(LeRobotStatsFile.from_json_dict(record["stats"]))
        stats = LeRobotStatsFile.aggregate(episode_stats)

        return LeRobotMetadata(info=info, stats=stats, tasks=tasks)

    def list_shards(self) -> list[Shard]:
        return self._json_reader().list_shards()

    def read_shard(self, shard: Shard) -> Iterator[SourceUnit]:
        # TODO: Given a Shard, read the specific episode parquet files and yield LeRobotTabular
        raise NotImplementedError("read_shard not yet implemented for v2.1")

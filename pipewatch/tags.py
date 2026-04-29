"""Tag-based grouping and lookup for pipeline metrics."""

from dataclasses import dataclass, field
from typing import Dict, List, Optional, Set
from pipewatch.metrics import PipelineMetric


@dataclass
class TagIndex:
    """Maps tags to sets of pipeline names for fast lookup."""
    _index: Dict[str, Set[str]] = field(default_factory=dict)

    def add(self, pipeline_name: str, tags: List[str]) -> None:
        """Register a pipeline under each of its tags."""
        for tag in tags:
            self._index.setdefault(tag, set()).add(pipeline_name)

    def remove(self, pipeline_name: str, tags: List[str]) -> None:
        """Unregister a pipeline from the given tags."""
        for tag in tags:
            if tag in self._index:
                self._index[tag].discard(pipeline_name)
                if not self._index[tag]:
                    del self._index[tag]

    def pipelines_for_tag(self, tag: str) -> Set[str]:
        """Return all pipeline names associated with a tag."""
        return set(self._index.get(tag, set()))

    def all_tags(self) -> List[str]:
        """Return a sorted list of all known tags."""
        return sorted(self._index.keys())


def tag_metrics(
    metrics: List[PipelineMetric],
    tag_map: Dict[str, List[str]],
) -> Dict[str, List[PipelineMetric]]:
    """Group metrics by tag using a pipeline-name -> tags mapping.

    Args:
        metrics: List of PipelineMetric instances.
        tag_map: Dict mapping pipeline_name to a list of tag strings.

    Returns:
        Dict mapping each tag to the list of metrics carrying that tag.
    """
    result: Dict[str, List[PipelineMetric]] = {}
    for metric in metrics:
        tags = tag_map.get(metric.pipeline_name, [])
        for tag in tags:
            result.setdefault(tag, []).append(metric)
    return result


def filter_by_tag(
    metrics: List[PipelineMetric],
    tag: str,
    tag_map: Dict[str, List[str]],
) -> List[PipelineMetric]:
    """Return only the metrics whose pipeline carries the given tag."""
    tagged = tag_metrics(metrics, tag_map)
    return tagged.get(tag, [])

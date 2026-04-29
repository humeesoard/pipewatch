"""Tests for pipewatch.tags module."""

import pytest
from pipewatch.metrics import PipelineMetric
from pipewatch.tags import TagIndex, tag_metrics, filter_by_tag


def make_metric(name: str, errors: int = 0, processed: int = 100) -> PipelineMetric:
    return PipelineMetric(
        pipeline_name=name,
        records_processed=processed,
        error_count=errors,
        latency_ms=20.0,
    )


# --- TagIndex tests ---

def test_tag_index_add_and_lookup():
    idx = TagIndex()
    idx.add("pipeline_a", ["team:data", "env:prod"])
    assert "pipeline_a" in idx.pipelines_for_tag("team:data")
    assert "pipeline_a" in idx.pipelines_for_tag("env:prod")


def test_tag_index_multiple_pipelines_same_tag():
    idx = TagIndex()
    idx.add("pipeline_a", ["env:prod"])
    idx.add("pipeline_b", ["env:prod"])
    result = idx.pipelines_for_tag("env:prod")
    assert "pipeline_a" in result
    assert "pipeline_b" in result


def test_tag_index_missing_tag_returns_empty():
    idx = TagIndex()
    assert idx.pipelines_for_tag("nonexistent") == set()


def test_tag_index_remove_pipeline():
    idx = TagIndex()
    idx.add("pipeline_a", ["env:prod"])
    idx.remove("pipeline_a", ["env:prod"])
    assert "pipeline_a" not in idx.pipelines_for_tag("env:prod")


def test_tag_index_remove_cleans_up_empty_tag():
    idx = TagIndex()
    idx.add("pipeline_a", ["solo_tag"])
    idx.remove("pipeline_a", ["solo_tag"])
    assert "solo_tag" not in idx.all_tags()


def test_tag_index_all_tags_sorted():
    idx = TagIndex()
    idx.add("p1", ["zzz", "aaa", "mmm"])
    assert idx.all_tags() == ["aaa", "mmm", "zzz"]


# --- tag_metrics tests ---

def test_tag_metrics_groups_correctly():
    metrics = [make_metric("p1"), make_metric("p2"), make_metric("p3")]
    tag_map = {"p1": ["env:prod"], "p2": ["env:prod", "team:data"], "p3": ["team:data"]}
    result = tag_metrics(metrics, tag_map)
    assert len(result["env:prod"]) == 2
    assert len(result["team:data"]) == 2


def test_tag_metrics_untagged_pipeline_excluded():
    metrics = [make_metric("p1"), make_metric("p2")]
    tag_map = {"p1": ["env:prod"]}
    result = tag_metrics(metrics, tag_map)
    names_in_prod = [m.pipeline_name for m in result.get("env:prod", [])]
    assert "p2" not in names_in_prod


def test_tag_metrics_empty_inputs():
    assert tag_metrics([], {}) == {}


# --- filter_by_tag tests ---

def test_filter_by_tag_returns_matching():
    metrics = [make_metric("p1"), make_metric("p2")]
    tag_map = {"p1": ["critical"], "p2": ["low-priority"]}
    result = filter_by_tag(metrics, "critical", tag_map)
    assert len(result) == 1
    assert result[0].pipeline_name == "p1"


def test_filter_by_tag_missing_tag_returns_empty():
    metrics = [make_metric("p1")]
    tag_map = {"p1": ["env:prod"]}
    result = filter_by_tag(metrics, "env:staging", tag_map)
    assert result == []

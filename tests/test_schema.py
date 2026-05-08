"""Tests for src/nextflow_turret/schema.py and the /api/pipeline/schema endpoint."""
from __future__ import annotations

import json
from unittest.mock import patch, MagicMock

import pytest

from nextflow_turret.schema import (
    fetch_pipeline_schema,
    ParamSpec,
    _resolve_schema_url,
    _parse_schema,
)


# ─────────────────────────────────────────────────────────────────────────────
# Fixtures / helpers
# ─────────────────────────────────────────────────────────────────────────────

MINIMAL_SCHEMA: dict = {
    "$schema": "http://json-schema.org/draft-07/schema",
    "title": "nf-core/testpipe pipeline parameters",
    "definitions": {
        "input_output": {
            "title": "Input/output options",
            "required": ["input", "outdir"],
            "properties": {
                "input": {
                    "type": "string",
                    "description": "Path to samplesheet CSV",
                },
                "outdir": {
                    "type": "string",
                    "description": "Output directory",
                    "default": "results",
                },
            },
        },
        "options": {
            "title": "Options",
            "properties": {
                "max_cpus": {
                    "type": "integer",
                    "description": "Maximum CPUs",
                    "default": 16,
                },
                "save_trimmed": {
                    "type": "boolean",
                    "description": "Save trimmed reads",
                    "default": False,
                },
                "aligner": {
                    "type": "string",
                    "description": "Aligner to use",
                    "enum": ["star", "hisat2", "bwa"],
                    "default": "star",
                },
                "secret_key": {
                    "type": "string",
                    "description": "Hidden field",
                    "hidden": True,
                },
            },
        },
    },
    "allOf": [
        {"$ref": "#/definitions/input_output"},
        {"$ref": "#/definitions/options"},
    ],
}


# ─────────────────────────────────────────────────────────────────────────────
# Unit: URL resolution
# ─────────────────────────────────────────────────────────────────────────────

class TestResolveSchemaUrl:
    def test_short_form_github(self):
        url = _resolve_schema_url("nf-core/rnaseq")
        assert url == "https://raw.githubusercontent.com/nf-core/rnaseq/main/nextflow_schema.json"

    def test_short_form_with_inline_revision(self):
        url = _resolve_schema_url("nf-core/rnaseq@3.14.0")
        assert url is not None
        assert "3.14.0" in url

    def test_short_form_with_explicit_revision(self):
        url = _resolve_schema_url("nf-core/rnaseq", revision="3.14.0")
        assert url is not None
        assert "3.14.0" in url

    def test_full_github_url(self):
        url = _resolve_schema_url("https://github.com/nf-core/rnaseq")
        assert url == "https://raw.githubusercontent.com/nf-core/rnaseq/main/nextflow_schema.json"

    def test_full_github_url_git_suffix(self):
        url = _resolve_schema_url("https://github.com/nf-core/rnaseq.git")
        assert url is not None
        assert "raw.githubusercontent.com" in url

    def test_gitlab_url(self):
        url = _resolve_schema_url("https://gitlab.com/org/pipeline")
        assert url is not None
        assert "gitlab.com" in url
        assert "nextflow_schema.json" in url

    def test_local_path_absolute(self):
        assert _resolve_schema_url("/local/pipeline") is None

    def test_local_path_relative(self):
        assert _resolve_schema_url("./local/pipeline") is None


# ─────────────────────────────────────────────────────────────────────────────
# Unit: Schema parsing
# ─────────────────────────────────────────────────────────────────────────────

class TestParseSchema:
    @pytest.fixture(autouse=True)
    def params(self):
        self._params = _parse_schema(MINIMAL_SCHEMA)

    def _by_key(self, key: str) -> ParamSpec:
        return next(p for p in self._params if p.key == key)

    def test_returns_list_of_paramspec(self):
        assert all(isinstance(p, ParamSpec) for p in self._params)

    def test_required_fields_marked(self):
        assert self._by_key("input").required is True
        assert self._by_key("outdir").required is True

    def test_optional_fields_not_required(self):
        assert self._by_key("max_cpus").required is False

    def test_type_preserved(self):
        assert self._by_key("input").type == "string"
        assert self._by_key("max_cpus").type == "integer"
        assert self._by_key("save_trimmed").type == "boolean"

    def test_default_preserved(self):
        assert self._by_key("outdir").default == "results"
        assert self._by_key("max_cpus").default == 16
        assert self._by_key("save_trimmed").default is False

    def test_enum_choices_preserved(self):
        aligner = self._by_key("aligner")
        assert aligner.choices == ["star", "hisat2", "bwa"]

    def test_hidden_flag(self):
        assert self._by_key("secret_key").hidden is True

    def test_group_assignment(self):
        assert self._by_key("input").group == "input_output"
        assert self._by_key("max_cpus").group == "options"

    def test_group_title(self):
        assert self._by_key("input").group_title == "Input/output options"

    def test_no_duplicate_keys(self):
        keys = [p.key for p in self._params]
        assert len(keys) == len(set(keys))

    def test_empty_schema_returns_empty_list(self):
        assert _parse_schema({}) == []

    def test_top_level_properties(self):
        schema = {
            "properties": {"foo": {"type": "string"}},
            "required": ["foo"],
        }
        params = _parse_schema(schema)
        assert len(params) == 1
        assert params[0].key == "foo"
        assert params[0].required is True

    def test_to_dict(self):
        p = self._by_key("input")
        d = p.to_dict()
        assert d["key"]      == "input"
        assert d["required"] is True
        assert isinstance(d["choices"], list)


# ─────────────────────────────────────────────────────────────────────────────
# Unit: fetch_pipeline_schema
# ─────────────────────────────────────────────────────────────────────────────

class TestFetchPipelineSchema:
    def _mock_fetch(self, schema_dict: dict):
        """Return a context-manager patch that makes _fetch_url return schema_dict."""
        return patch(
            "nextflow_turret.schema._fetch_url",
            return_value=schema_dict,
        )

    def test_returns_params_on_valid_schema(self):
        with self._mock_fetch(MINIMAL_SCHEMA):
            params = fetch_pipeline_schema("nf-core/testpipe")
        assert len(params) > 0

    def test_returns_empty_on_none(self):
        with patch("nextflow_turret.schema._fetch_url", return_value=None):
            params = fetch_pipeline_schema("nf-core/missing")
        assert params == []

    def test_master_fallback(self):
        """When 'main' returns None, should try 'master'."""
        call_count = [0]

        def side_effect(url, **kwargs):
            call_count[0] += 1
            if "main" in url:
                return None
            return MINIMAL_SCHEMA

        with patch("nextflow_turret.schema._fetch_url", side_effect=side_effect):
            params = fetch_pipeline_schema("nf-core/testpipe")

        assert call_count[0] == 2
        assert len(params) > 0

    def test_local_path_missing_schema(self, tmp_path):
        params = fetch_pipeline_schema(str(tmp_path))
        assert params == []

    def test_local_path_valid_schema(self, tmp_path):
        schema_file = tmp_path / "nextflow_schema.json"
        schema_file.write_text(json.dumps(MINIMAL_SCHEMA))
        params = fetch_pipeline_schema(str(tmp_path))
        assert len(params) > 0

    def test_local_path_invalid_json(self, tmp_path):
        (tmp_path / "nextflow_schema.json").write_text("not json!!")
        assert fetch_pipeline_schema(str(tmp_path)) == []

    def test_unknown_pipeline_format_returns_empty(self):
        with patch("nextflow_turret.schema._fetch_url", return_value=None):
            params = fetch_pipeline_schema("not-a-valid-pipeline-string")
        assert params == []


# ─────────────────────────────────────────────────────────────────────────────
# Integration: /api/pipeline/schema endpoint
# ─────────────────────────────────────────────────────────────────────────────

# ─────────────────────────────────────────────────────────────────────────────
# Integration: /api/pipeline/schema endpoint
# ─────────────────────────────────────────────────────────────────────────────

class TestSchemApiEndpoint:
    def test_returns_params_when_schema_found(self, mem_client):
        with patch(
            "nextflow_turret.schema._fetch_url",
            return_value=MINIMAL_SCHEMA,
        ):
            resp = mem_client.get(
                "/api/pipeline/schema",
                params={"pipeline": "nf-core/testpipe"},
            )
        assert resp.status_code == 200
        data = resp.json()
        assert data["count"] > 0
        assert data["source"] == "nextflow_schema.json"
        assert isinstance(data["params"], list)
        keys = [p["key"] for p in data["params"]]
        assert "input"  in keys
        assert "outdir" in keys

    def test_returns_empty_when_schema_missing(self, mem_client):
        with patch("nextflow_turret.schema._fetch_url", return_value=None):
            resp = mem_client.get(
                "/api/pipeline/schema",
                params={"pipeline": "nf-core/missing"},
            )
        assert resp.status_code == 200
        data = resp.json()
        assert data["count"]  == 0
        assert data["source"] is None
        assert data["params"] == []

    def test_revision_forwarded(self, mem_client):
        captured = {}

        def mock_fetch(url, **kwargs):
            captured["url"] = url
            return MINIMAL_SCHEMA

        with patch("nextflow_turret.schema._fetch_url", side_effect=mock_fetch):
            mem_client.get(
                "/api/pipeline/schema",
                params={"pipeline": "nf-core/testpipe", "revision": "3.14.0"},
            )

        assert "3.14.0" in captured.get("url", "")

    def test_missing_pipeline_param_returns_422(self, mem_client):
        resp = mem_client.get("/api/pipeline/schema")
        assert resp.status_code == 422

    def test_param_fields_have_expected_keys(self, mem_client):
        with patch("nextflow_turret.schema._fetch_url", return_value=MINIMAL_SCHEMA):
            resp = mem_client.get(
                "/api/pipeline/schema",
                params={"pipeline": "nf-core/testpipe"},
            )
        for p in resp.json()["params"]:
            for field in ("key", "type", "description", "required", "hidden", "choices", "default"):
                assert field in p, f"Missing field {field!r} in {p}"


# ─────────────────────────────────────────────────────────────────────────────
# Integration: POST /launch with individual param__ fields
# ─────────────────────────────────────────────────────────────────────────────

class TestLaunchFormParamFields:
    def test_individual_param_fields_accepted(self, mem_client):
        """param__KEY fields should be collected into the params dict."""
        resp = mem_client.post(
            "/launch",
            data={
                "pipeline":       "nf-core/testpipe",
                "param__input":   "s3://bucket/samplesheet.csv",
                "param__outdir":  "results",
            },
            follow_redirects=False,
        )
        # Should redirect to /launches/{id} on success
        assert resp.status_code == 303
        assert resp.headers["location"].startswith("/launches/")

    def test_json_textarea_still_works(self, mem_client):
        """Backwards-compat: JSON textarea params field still accepted."""
        resp = mem_client.post(
            "/launch",
            data={
                "pipeline": "nf-core/testpipe",
                "params":   '{"input": "s3://bucket/sheet.csv", "outdir": "results"}',
            },
            follow_redirects=False,
        )
        assert resp.status_code == 303

    def test_invalid_json_returns_422(self, mem_client):
        resp = mem_client.post(
            "/launch",
            data={
                "pipeline": "nf-core/testpipe",
                "params":   "{bad json!!}",
            },
            follow_redirects=False,
        )
        assert resp.status_code == 422

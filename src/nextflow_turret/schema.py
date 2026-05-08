"""Fetch and parse ``nextflow_schema.json`` for a pipeline.

Nextflow pipelines (especially nf-core ones) publish a ``nextflow_schema.json``
file at the repo root.  This module locates and parses it into a flat list of
:class:`ParamSpec` objects, one per pipeline parameter.

Supported pipeline formats
--------------------------
- ``nf-core/rnaseq`` or ``nf-core/rnaseq@3.14.0``
- ``org/repo`` (resolves to GitHub)
- ``https://github.com/org/repo``
- ``https://gitlab.com/org/repo``
- ``https://bitbucket.org/org/repo``
- Local path ``/path/to/pipeline`` (reads file directly)
"""
from __future__ import annotations

import json
import re
import urllib.error
import urllib.request
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Optional


# ---------------------------------------------------------------------------
# Data model
# ---------------------------------------------------------------------------

@dataclass
class ParamSpec:
    """A single pipeline parameter extracted from nextflow_schema.json."""
    key:         str
    type:        str                    # string | number | integer | boolean
    description: str                    = ""
    help_text:   str                    = ""
    default:     Any                    = None
    required:    bool                   = False
    hidden:      bool                   = False
    choices:     list[str]              = field(default_factory=list)
    group:       str                    = ""
    group_title: str                    = ""

    def to_dict(self) -> dict:
        return {
            "key":         self.key,
            "type":        self.type,
            "description": self.description,
            "help_text":   self.help_text,
            "default":     self.default,
            "required":    self.required,
            "hidden":      self.hidden,
            "choices":     self.choices,
            "group":       self.group,
            "group_title": self.group_title,
        }


# ---------------------------------------------------------------------------
# Schema URL resolution
# ---------------------------------------------------------------------------

def _raw_github_url(owner: str, repo: str, revision: str = "main") -> str:
    return f"https://raw.githubusercontent.com/{owner}/{repo}/{revision}/nextflow_schema.json"


def _resolve_schema_url(pipeline: str, revision: Optional[str] = None) -> Optional[str]:
    """Return a raw URL for ``nextflow_schema.json``, or ``None`` for local paths."""
    rev = revision or "main"

    # Local path
    if pipeline.startswith("/") or pipeline.startswith("."):
        return None

    # Full https:// URL (GitHub / GitLab / Bitbucket)
    m = re.match(
        r"https?://(github\.com|gitlab\.com|bitbucket\.org)/([^/]+)/([^/\s]+?)(?:\.git)?/?$",
        pipeline.strip(),
    )
    if m:
        host, owner, repo = m.group(1), m.group(2), m.group(3)
        if host == "github.com":
            return _raw_github_url(owner, repo, rev)
        # GitLab raw URL format
        if host == "gitlab.com":
            return f"https://gitlab.com/{owner}/{repo}/-/raw/{rev}/nextflow_schema.json"
        # Bitbucket
        if host == "bitbucket.org":
            return f"https://bitbucket.org/{owner}/{repo}/raw/{rev}/nextflow_schema.json"

    # Short form: "owner/repo" or "owner/repo@revision"
    short = re.match(r"^([A-Za-z0-9_.-]+)/([A-Za-z0-9_.-]+?)(?:@([^\s]+))?$", pipeline.strip())
    if short:
        owner, repo, inline_rev = short.group(1), short.group(2), short.group(3)
        return _raw_github_url(owner, repo, inline_rev or rev)

    return None


# ---------------------------------------------------------------------------
# Fetch + parse
# ---------------------------------------------------------------------------

def _fetch_url(url: str, timeout: int = 8) -> Optional[dict]:
    """GET *url* and parse response as JSON.  Returns ``None`` on any error."""
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "nextflow-turret/0.1"})
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return json.loads(resp.read().decode())
    except (urllib.error.HTTPError, urllib.error.URLError, json.JSONDecodeError, OSError):
        return None


def _parse_schema(schema: dict) -> list[ParamSpec]:
    """Extract :class:`ParamSpec` objects from a ``nextflow_schema.json`` dict."""
    params: list[ParamSpec] = []
    seen:   set[str]        = set()

    # Collect required keys per definition group
    def _required_in(defn: dict) -> set[str]:
        return set(defn.get("required", []))

    # Walk definitions referenced by allOf / properties
    definitions = schema.get("definitions", schema.get("$defs", {}))

    # Determine iteration order from allOf
    refs_ordered: list[str] = []
    for entry in schema.get("allOf", []):
        ref = entry.get("$ref", "")
        m = re.search(r"[#/](\w+)$", ref)
        if m and m.group(1) in definitions:
            refs_ordered.append(m.group(1))

    # Fall back to all definition keys if no allOf
    if not refs_ordered:
        refs_ordered = list(definitions.keys())

    for group_key in refs_ordered:
        defn         = definitions[group_key]
        group_title  = defn.get("title", group_key.replace("_", " ").title())
        required_set = _required_in(defn)
        props        = defn.get("properties", {})

        for pkey, pval in props.items():
            if pkey in seen:
                continue
            seen.add(pkey)

            ptype   = pval.get("type", "string")
            choices = pval.get("enum", [])

            params.append(ParamSpec(
                key         = pkey,
                type        = ptype,
                description = pval.get("description", ""),
                help_text   = pval.get("help_text", ""),
                default     = pval.get("default"),
                required    = pkey in required_set,
                hidden      = pval.get("hidden", False),
                choices     = [str(c) for c in choices],
                group       = group_key,
                group_title = group_title,
            ))

    # Also handle top-level properties (pipelines without definitions)
    for pkey, pval in schema.get("properties", {}).items():
        if pkey in seen:
            continue
        seen.add(pkey)
        params.append(ParamSpec(
            key         = pkey,
            type        = pval.get("type", "string"),
            description = pval.get("description", ""),
            default     = pval.get("default"),
            required    = pkey in set(schema.get("required", [])),
            hidden      = pval.get("hidden", False),
            choices     = [str(c) for c in pval.get("enum", [])],
        ))

    return params


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def fetch_pipeline_schema(
    pipeline: str,
    revision: Optional[str] = None,
    timeout:  int           = 8,
) -> list[ParamSpec]:
    """Return pipeline parameters as :class:`ParamSpec` objects.

    Returns an empty list when the schema cannot be found or parsed.
    """
    # Local path
    if pipeline.startswith("/") or pipeline.startswith("."):
        schema_file = Path(pipeline) / "nextflow_schema.json"
        if not schema_file.is_file():
            return []
        try:
            schema = json.loads(schema_file.read_text())
        except (json.JSONDecodeError, OSError):
            return []
        return _parse_schema(schema)

    url = _resolve_schema_url(pipeline, revision)
    if url is None:
        return []

    schema = _fetch_url(url, timeout=timeout)
    if schema is None:
        # Try with "master" fallback if revision was "main"
        if not revision or revision == "main":
            schema = _fetch_url(
                _resolve_schema_url(pipeline, "master") or url,
                timeout=timeout,
            )
    if schema is None:
        return []

    return _parse_schema(schema)

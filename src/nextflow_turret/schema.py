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

# Nextflow schema format values that indicate a file or directory path.
_FILE_PATH_FORMATS:      frozenset[str] = frozenset({"file-path", "file-path-pattern"})
_DIRECTORY_PATH_FORMATS: frozenset[str] = frozenset({"directory-path"})
_PATH_FORMATS:           frozenset[str] = _FILE_PATH_FORMATS | _DIRECTORY_PATH_FORMATS | {"path"}


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
    format:      str                    = ""  # e.g. "file-path", "directory-path"

    @property
    def is_file_path(self) -> bool:
        return self.format in _FILE_PATH_FORMATS

    @property
    def is_directory_path(self) -> bool:
        return self.format in _DIRECTORY_PATH_FORMATS

    @property
    def is_path(self) -> bool:
        return self.format in _PATH_FORMATS

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
            "format":      self.format,
        }


# ---------------------------------------------------------------------------
# Schema URL resolution
# ---------------------------------------------------------------------------

def _raw_github_url(owner: str, repo: str, revision: str = "main", filename: str = "nextflow_schema.json") -> str:
    return f"https://raw.githubusercontent.com/{owner}/{repo}/{revision}/{filename}"


def _resolve_file_url(pipeline: str, filename: str, revision: Optional[str] = None) -> Optional[str]:
    """Return a raw URL for *filename* in the pipeline repo, or ``None`` for local paths."""
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
            return _raw_github_url(owner, repo, rev, filename)
        if host == "gitlab.com":
            return f"https://gitlab.com/{owner}/{repo}/-/raw/{rev}/{filename}"
        if host == "bitbucket.org":
            return f"https://bitbucket.org/{owner}/{repo}/raw/{rev}/{filename}"

    # Short form: "owner/repo" or "owner/repo@revision"
    short = re.match(r"^([A-Za-z0-9_.-]+)/([A-Za-z0-9_.-]+?)(?:@([^\s]+))?$", pipeline.strip())
    if short:
        owner, repo, inline_rev = short.group(1), short.group(2), short.group(3)
        return _raw_github_url(owner, repo, inline_rev or rev, filename)

    return None


def _resolve_schema_url(pipeline: str, revision: Optional[str] = None) -> Optional[str]:
    return _resolve_file_url(pipeline, "nextflow_schema.json", revision)


def _resolve_config_url(pipeline: str, revision: Optional[str] = None) -> Optional[str]:
    return _resolve_file_url(pipeline, "nextflow.config", revision)


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


def _fetch_text_url(url: str, timeout: int = 8) -> Optional[str]:
    """GET *url* and return the response body as text.  Returns ``None`` on any error."""
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "nextflow-turret/0.1"})
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return resp.read().decode(errors="replace")
    except (urllib.error.HTTPError, urllib.error.URLError, OSError):
        return None


def _parse_profiles(config_text: str) -> list[str]:
    """Extract profile names from the text of a ``nextflow.config`` file.

    Handles nested braces correctly by tracking depth, so nested settings
    inside a profile block are not mistaken for additional profiles.
    """
    # Strip single-line comments to avoid matching commented-out profiles
    text = re.sub(r'//[^\n]*', '', config_text)

    m = re.search(r'\bprofiles\s*\{', text)
    if not m:
        return []

    # Find matching closing brace of the profiles { } block
    start = m.end()
    depth = 1
    i = start
    while i < len(text) and depth > 0:
        if text[i] == '{':
            depth += 1
        elif text[i] == '}':
            depth -= 1
        i += 1
    block = text[start : i - 1]

    # Scan block at depth 0; identifiers directly followed by '{' are profile names
    names: list[str] = []
    j = 0
    depth = 0
    while j < len(block):
        c = block[j]
        if c == '{':
            depth += 1
            j += 1
        elif c == '}':
            depth -= 1
            j += 1
        elif depth == 0:
            m2 = re.match(r'([A-Za-z_]\w*)\s*\{', block[j:])
            if m2:
                names.append(m2.group(1))
                j += len(m2.group(1))  # skip past the identifier; next char is space or '{'
            else:
                j += 1
        else:
            j += 1

    return names


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
                format      = pval.get("format", ""),
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
            format      = pval.get("format", ""),
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


def fetch_pipeline_refs(
    pipeline: str,
    timeout:  int = 8,
) -> dict[str, list[str]]:
    """Return ``{"branches": [...], "tags": [...]}`` for a remote pipeline.

    Uses the GitHub REST API for GitHub-hosted pipelines.  Returns empty lists
    for local paths or pipelines on unsupported hosts.
    """
    empty: dict[str, list[str]] = {"branches": [], "tags": []}

    if pipeline.startswith("/") or pipeline.startswith("."):
        return empty

    # Parse owner/repo from URL or short form
    m = re.match(
        r"https?://github\.com/([^/]+)/([^/\s]+?)(?:\.git)?/?$",
        pipeline.strip(),
    )
    if m:
        owner, repo = m.group(1), m.group(2)
    else:
        short = re.match(r"^([A-Za-z0-9_.-]+)/([A-Za-z0-9_.-]+?)(?:@[^\s]+)?$", pipeline.strip())
        if not short:
            return empty
        owner, repo = short.group(1), short.group(2)

    def _gh_list(endpoint: str) -> list[str]:
        url = f"https://api.github.com/repos/{owner}/{repo}/{endpoint}?per_page=100"
        data = _fetch_url(url, timeout=timeout)
        if not isinstance(data, list):
            return []
        return [item.get("name", "") for item in data if item.get("name")]

    branches = _gh_list("branches")
    tags     = _gh_list("tags")
    return {"branches": branches, "tags": tags}


def fetch_pipeline_profiles(
    pipeline: str,
    revision: Optional[str] = None,
    timeout:  int           = 8,
) -> list[str]:
    """Return profile names defined in ``nextflow.config`` for a pipeline.

    Returns an empty list when the config cannot be found or parsed.
    """
    # Local path
    if pipeline.startswith("/") or pipeline.startswith("."):
        config_file = Path(pipeline) / "nextflow.config"
        if not config_file.is_file():
            return []
        try:
            return _parse_profiles(config_file.read_text(errors="replace"))
        except OSError:
            return []

    url = _resolve_config_url(pipeline, revision)
    if url is None:
        return []

    text = _fetch_text_url(url, timeout=timeout)
    if text is None and (not revision or revision == "main"):
        text = _fetch_text_url(
            _resolve_config_url(pipeline, "master") or url,
            timeout=timeout,
        )
    if text is None:
        return []

    return _parse_profiles(text)

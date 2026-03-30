#!/usr/bin/env python3
"""
Zendesk Help Center Migration: Production → Sandbox
=====================================================
Migrates brands, categories, sections, and articles (including translations
and inline attachments) from a production Zendesk instance to a sandbox.

Usage:
    1. Fill in credentials in the CONFIG section below (or use env vars).
    2. pip install requests
    3. python zendesk_hc_migration.py

Features:
    - Migrates brands, categories, sections, articles (in dependency order)
    - Preserves hierarchy (brand → category → section → article)
    - Handles article body inline-image re-hosting
    - Rate-limit aware with automatic retry + back-off
    - Generates a full ID mapping report (production ID → sandbox ID)
    - Dry-run mode to preview without writing
    - Resumable: skips already-migrated objects when re-run
    - Detailed logging to console + file

Author: Zendesk Help Center Migrator Contributors
"""

import os
import sys
import json
import time
import re
import logging
import csv
import hashlib
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Optional

try:
    import requests
except ImportError:
    sys.exit("ERROR: 'requests' library required. Install with:  pip install requests")


# ╔══════════════════════════════════════════════════════════════════════════════╗
# ║  CONFIGURATION                                                              ║
# ╚══════════════════════════════════════════════════════════════════════════════╝

CONFIG = {
    # ── Production (source) ──────────────────────────────────────────────────
    "prod_subdomain":   os.getenv("ZD_PROD_SUBDOMAIN",   "YOUR_PROD_SUBDOMAIN"),
    "prod_email":       os.getenv("ZD_PROD_EMAIL",       "you@company.com"),
    "prod_api_token":   os.getenv("ZD_PROD_TOKEN",       "YOUR_PROD_API_TOKEN"),

    # ── Sandbox (destination) ────────────────────────────────────────────────
    "sand_subdomain":   os.getenv("ZD_SAND_SUBDOMAIN",   "YOUR_SANDBOX_SUBDOMAIN"),
    "sand_email":       os.getenv("ZD_SAND_EMAIL",       "you@company.com"),
    "sand_api_token":   os.getenv("ZD_SAND_TOKEN",       "YOUR_SAND_API_TOKEN"),

    # ── Migration options ────────────────────────────────────────────────────
    "dry_run":          os.getenv("ZD_DRY_RUN", "false").lower() == "true",
    "migrate_brands":   True,
    "migrate_categories": True,
    "migrate_sections": True,
    "migrate_articles": True,
    "migrate_translations": True,
    "migrate_attachments": True,
    "export_csv":         True,   # save all fetched data locally as CSV
    "csv_output_dir":     "hc_csv_export",  # folder for CSV files

    # ── Rate limiting ────────────────────────────────────────────────────────
    "requests_per_minute": 80,  # stay under Zendesk's 100 RPM for token auth
    "retry_max":           5,
    "retry_backoff_base":  2,   # exponential back-off base (seconds)

    # ── Output ───────────────────────────────────────────────────────────────
    "mapping_file":     "migration_id_mapping.json",
    "log_file":         "migration.log",
}


# ╔══════════════════════════════════════════════════════════════════════════════╗
# ║  LOGGING SETUP                                                              ║
# ╚══════════════════════════════════════════════════════════════════════════════╝

log = logging.getLogger("zd_migration")
log.setLevel(logging.DEBUG)

_fmt = logging.Formatter("[%(asctime)s] %(levelname)-7s %(message)s", "%Y-%m-%d %H:%M:%S")

_ch = logging.StreamHandler()
_ch.setLevel(logging.INFO)
_ch.setFormatter(_fmt)
log.addHandler(_ch)

_fh = logging.FileHandler(CONFIG["log_file"], mode="a")
_fh.setLevel(logging.DEBUG)
_fh.setFormatter(_fmt)
log.addHandler(_fh)


# ╔══════════════════════════════════════════════════════════════════════════════╗
# ║  ZENDESK API CLIENT                                                         ║
# ╚══════════════════════════════════════════════════════════════════════════════╝

class ZendeskClient:
    """Thin wrapper around the Zendesk REST API with rate-limit handling."""

    def __init__(self, subdomain: str, email: str, api_token: str, label: str = ""):
        self.base_url = f"https://{subdomain}.zendesk.com"
        self.subdomain = subdomain
        self.email = email
        self.api_token = api_token
        self.auth = (f"{email}/token", api_token)
        self.label = label or subdomain
        self._min_interval = 60.0 / CONFIG["requests_per_minute"]
        self._last_request_time = 0.0

    def for_brand(self, brand_subdomain: str, label_suffix: str = "") -> "ZendeskClient":
        """Return a new client pointing at a brand-specific subdomain.

        In multi-brand Zendesk, each brand has its own subdomain
        (e.g. 'brand1' → brand1.zendesk.com). Help Center API calls
        must be made against the brand's subdomain to see that brand's
        categories, sections, and articles.
        """
        new_label = f"{self.label}/{label_suffix}" if label_suffix else f"{self.label}/{brand_subdomain}"
        return ZendeskClient(brand_subdomain, self.email, self.api_token, new_label)

    # ── Core request with retry + rate-limit ─────────────────────────────────

    def _request(self, method: str, endpoint: str, **kwargs) -> requests.Response:
        url = f"{self.base_url}{endpoint}"
        for attempt in range(1, CONFIG["retry_max"] + 1):
            # Throttle
            elapsed = time.time() - self._last_request_time
            if elapsed < self._min_interval:
                time.sleep(self._min_interval - elapsed)

            self._last_request_time = time.time()
            log.debug(f"[{self.label}] {method} {endpoint}  (attempt {attempt})")

            try:
                resp = requests.request(method, url, auth=self.auth, timeout=30, **kwargs)
            except requests.RequestException as exc:
                log.warning(f"[{self.label}] Network error: {exc}")
                time.sleep(CONFIG["retry_backoff_base"] ** attempt)
                continue

            # Rate-limited → honour Retry-After
            if resp.status_code == 429:
                wait = int(resp.headers.get("Retry-After", CONFIG["retry_backoff_base"] ** attempt))
                log.warning(f"[{self.label}] Rate-limited. Waiting {wait}s …")
                time.sleep(wait)
                continue

            if resp.status_code >= 500:
                log.warning(f"[{self.label}] Server error {resp.status_code}. Retrying …")
                self._log_error_detail(method, url, resp, kwargs)
                time.sleep(CONFIG["retry_backoff_base"] ** attempt)
                continue

            # Log detailed diagnostics for any client error (4xx)
            if resp.status_code >= 400:
                self._log_error_detail(method, url, resp, kwargs)

            return resp

        log.error(f"[{self.label}] Exhausted retries for {method} {endpoint}")
        raise RuntimeError(f"Failed after {CONFIG['retry_max']} retries: {method} {endpoint}")

    def _log_error_detail(self, method: str, url: str, resp: requests.Response, kwargs: dict):
        """Log comprehensive diagnostic information for a failed API call."""
        log.error(f"[{self.label}] ┌── API ERROR ──────────────────────────────────")
        log.error(f"[{self.label}] │ Request : {method} {url}")
        log.error(f"[{self.label}] │ Status  : {resp.status_code} {resp.reason}")

        # Response body (Zendesk usually returns JSON with error details)
        try:
            err_json = resp.json()
            # Zendesk error formats: {"error": ...}, {"errors": [...]}, {"details": {...}}
            if "error" in err_json:
                log.error(f"[{self.label}] │ Error   : {err_json['error']}")
            if "message" in err_json:
                log.error(f"[{self.label}] │ Message : {err_json['message']}")
            if "description" in err_json:
                log.error(f"[{self.label}] │ Desc    : {err_json['description']}")
            if "errors" in err_json:
                for i, e in enumerate(err_json["errors"][:5]):
                    log.error(f"[{self.label}] │ Error[{i}]: {e}")
            if "details" in err_json:
                for field, msgs in err_json["details"].items():
                    log.error(f"[{self.label}] │ Detail  : {field} → {msgs}")
        except (ValueError, KeyError):
            # Not JSON — log raw text
            log.error(f"[{self.label}] │ Body    : {resp.text[:800]}")

        # Request payload (redact auth, keep the JSON body for debugging)
        if "json" in kwargs:
            payload_str = json.dumps(kwargs["json"], indent=None, default=str)
            # Truncate very long payloads (article bodies)
            if len(payload_str) > 1000:
                payload_str = payload_str[:1000] + " … [truncated]"
            log.error(f"[{self.label}] │ Payload : {payload_str}")
        elif "data" in kwargs:
            log.error(f"[{self.label}] │ Data    : {str(kwargs['data'])[:500]}")
        if "files" in kwargs:
            file_names = [name for name, _ in kwargs["files"].items()] if isinstance(kwargs["files"], dict) else [f[0] for f in kwargs["files"]]
            log.error(f"[{self.label}] │ Files   : {file_names}")

        # Response headers that help with debugging
        req_id = resp.headers.get("x-request-id", resp.headers.get("X-Zendesk-Request-Id", ""))
        if req_id:
            log.error(f"[{self.label}] │ Req ID  : {req_id}")
        retry_after = resp.headers.get("Retry-After", "")
        if retry_after:
            log.error(f"[{self.label}] │ Retry   : {retry_after}s")

        log.error(f"[{self.label}] └───────────────────────────────────────────────")

    def get(self, endpoint, **kw):
        return self._request("GET", endpoint, **kw)

    def post(self, endpoint, **kw):
        return self._request("POST", endpoint, **kw)

    def put(self, endpoint, **kw):
        return self._request("PUT", endpoint, **kw)

    # ── Paginated list ───────────────────────────────────────────────────────

    def get_all(self, endpoint: str, collection_key: str) -> list:
        """Fetch all pages of a paginated Zendesk list endpoint."""
        results = []
        url = endpoint
        while url:
            resp = self.get(url)
            resp.raise_for_status()
            data = resp.json()
            results.extend(data.get(collection_key, []))
            url = data.get("next_page")
            if url:
                # next_page is a full URL; convert to relative endpoint
                url = url.replace(self.base_url, "")
        return results


# ╔══════════════════════════════════════════════════════════════════════════════╗
# ║  MIGRATION ENGINE                                                           ║
# ╚══════════════════════════════════════════════════════════════════════════════╝

class HelpCenterMigration:
    """Orchestrates the full Help Center migration pipeline."""

    def __init__(self):
        self.prod = ZendeskClient(
            CONFIG["prod_subdomain"], CONFIG["prod_email"], CONFIG["prod_api_token"], "PROD"
        )
        self.sand = ZendeskClient(
            CONFIG["sand_subdomain"], CONFIG["sand_email"], CONFIG["sand_api_token"], "SAND"
        )
        self.dry_run = CONFIG["dry_run"]
        self._test_only = False  # set via CLI --test-only

        # ID mappings:  { "brands": {prod_id: sand_id}, "categories": {…}, … }
        self.mapping = self._load_mapping()

        # Stats
        self.stats = {
            "brands": {"created": 0, "skipped": 0, "failed": 0},
            "categories": {"created": 0, "skipped": 0, "failed": 0},
            "sections": {"created": 0, "skipped": 0, "failed": 0},
            "articles": {"created": 0, "skipped": 0, "failed": 0},
            "translations": {"created": 0, "skipped": 0, "failed": 0},
            "attachments": {"created": 0, "skipped": 0, "failed": 0},
        }

        # Raw data collectors for CSV export
        self._raw_brands: list[dict] = []
        self._raw_categories: list[dict] = []
        self._raw_sections: list[dict] = []
        self._raw_articles: list[dict] = []

        # Brand subdomain map: {prod_brand_id: {"prod_sub": ..., "sand_sub": ...}}
        self._brand_subdomains: dict[str, dict] = {}

        # Sandbox permission group & user segment (fetched lazily)
        self._sand_permission_group_id: Optional[int] = None
        self._sand_user_segment_id: Optional[int] = None

    def _ensure_sandbox_permission_group(self):
        """Fetch the first available permission group and user segment from sandbox."""
        if self._sand_permission_group_id is not None:
            return

        # Permission groups
        try:
            resp = self.sand.get("/api/v2/guide/permission_groups")
            if resp.status_code == 200:
                groups = resp.json().get("permission_groups", [])
                if groups:
                    self._sand_permission_group_id = groups[0]["id"]
                    log.info(f"  Sandbox permission_group_id: {self._sand_permission_group_id}")
        except Exception as exc:
            log.warning(f"  Could not fetch permission groups: {exc}")

        # User segments
        try:
            resp = self.sand.get("/api/v2/help_center/user_segments")
            if resp.status_code == 200:
                segments = resp.json().get("user_segments", [])
                # Prefer "Everyone" or the first built-in segment
                for seg in segments:
                    if seg.get("user_type") == "everyone" or seg.get("built_in"):
                        self._sand_user_segment_id = seg["id"]
                        break
                if not self._sand_user_segment_id and segments:
                    self._sand_user_segment_id = segments[0]["id"]
                log.info(f"  Sandbox user_segment_id: {self._sand_user_segment_id}")
        except Exception as exc:
            log.warning(f"  Could not fetch user segments: {exc}")

    # ── Mapping persistence ──────────────────────────────────────────────────

    def _load_mapping(self) -> dict:
        path = Path(CONFIG["mapping_file"])
        if path.exists():
            log.info(f"Loading existing mapping from {path}")
            with open(path) as f:
                return json.load(f)
        return {
            "brands": {},
            "categories": {},
            "sections": {},
            "articles": {},
            "attachments": {},
        }

    def _save_mapping(self):
        with open(CONFIG["mapping_file"], "w") as f:
            json.dump(self.mapping, f, indent=2)

    def _mapped(self, kind: str, prod_id) -> Optional[int]:
        return self.mapping[kind].get(str(prod_id))

    def _record(self, kind: str, prod_id, sand_id):
        self.mapping[kind][str(prod_id)] = sand_id
        self._save_mapping()

    # ── 1. BRANDS ────────────────────────────────────────────────────────────

    def migrate_brands(self):
        if not CONFIG["migrate_brands"]:
            return
        log.info("═" * 60)
        log.info("PHASE 1: Migrating Brands")
        log.info("═" * 60)

        prod_brands = self.prod.get_all("/api/v2/brands", "brands")
        self._raw_brands = prod_brands  # store for CSV export
        sand_brands = self.sand.get_all("/api/v2/brands", "brands")
        sand_brand_names = {b["name"].lower(): b for b in sand_brands}

        for brand in prod_brands:
            pid = brand["id"]
            prod_sub = brand.get("subdomain", CONFIG["prod_subdomain"])

            # Already mapped from a previous run?
            if self._mapped("brands", pid):
                log.info(f"  [SKIP] Brand '{brand['name']}' already migrated")
                self.stats["brands"]["skipped"] += 1
                # Still need subdomain mapping for later phases
                sand_obj = sand_brand_names.get(brand["name"].lower())
                sand_sub = sand_obj.get("subdomain", CONFIG["sand_subdomain"]) if sand_obj else CONFIG["sand_subdomain"]
                self._brand_subdomains[str(pid)] = {"prod_sub": prod_sub, "sand_sub": sand_sub}
                continue

            # Match by name in sandbox (sandbox often has default brand)
            if brand["name"].lower() in sand_brand_names:
                sand_obj = sand_brand_names[brand["name"].lower()]
                sid = sand_obj["id"]
                sand_sub = sand_obj.get("subdomain", CONFIG["sand_subdomain"])
                log.info(f"  [MAP]  Brand '{brand['name']}' matched existing sandbox brand {sid}")
                self._record("brands", pid, sid)
                self._brand_subdomains[str(pid)] = {"prod_sub": prod_sub, "sand_sub": sand_sub}
                self.stats["brands"]["skipped"] += 1
                continue

            if self.dry_run:
                log.info(f"  [DRY]  Would create brand '{brand['name']}'")
                self._brand_subdomains[str(pid)] = {"prod_sub": prod_sub, "sand_sub": prod_sub}
                self.stats["brands"]["skipped"] += 1
                continue

            payload = {
                "brand": {
                    "name": brand["name"],
                    "subdomain": brand.get("subdomain", ""),
                    "active": brand.get("active", True),
                    "has_help_center": brand.get("has_help_center", True),
                    "brand_url": brand.get("brand_url", ""),
                    "host_mapping": "",  # clear prod host mapping
                }
            }
            try:
                resp = self.sand.post("/api/v2/brands", json=payload)
                resp.raise_for_status()
                new_brand = resp.json()["brand"]
                self._record("brands", pid, new_brand["id"])
                sand_sub = new_brand.get("subdomain", CONFIG["sand_subdomain"])
                self._brand_subdomains[str(pid)] = {"prod_sub": prod_sub, "sand_sub": sand_sub}
                log.info(f"  [OK]   Brand '{brand['name']}' → sandbox id {new_brand['id']} (sub: {sand_sub})")
                self.stats["brands"]["created"] += 1

                # Enable Help Center on new brand
                self.sand.put(
                    f"/api/v2/brands/{new_brand['id']}",
                    json={"brand": {"has_help_center": True}},
                )
            except Exception as exc:
                log.error(f"  [FAIL] Brand '{brand['name']}': {exc}")
                self.stats["brands"]["failed"] += 1

        log.info(f"  Brand subdomain map: { {k: v['prod_sub'] for k, v in self._brand_subdomains.items()} }")

    # ── 2. CATEGORIES ────────────────────────────────────────────────────────

    def migrate_categories(self):
        if not CONFIG["migrate_categories"]:
            return
        log.info("═" * 60)
        log.info("PHASE 2: Migrating Categories")
        log.info("═" * 60)

        # In multi-brand Zendesk, each brand has its own HC subdomain.
        # We must query each brand's subdomain to get its categories.
        # For sandbox WRITES, we always use the main sandbox subdomain —
        # sandbox brands share the account subdomain, not separate ones.
        for prod_brand_id, sand_brand_id in self.mapping["brands"].items():
            subs = self._brand_subdomains.get(str(prod_brand_id), {})
            prod_sub = subs.get("prod_sub", CONFIG["prod_subdomain"])

            prod_brand_client = self.prod.for_brand(prod_sub, f"brand-{prod_brand_id}")

            log.info(f"  Brand {prod_brand_id} ({prod_sub}) → {sand_brand_id}")

            try:
                categories = prod_brand_client.get_all(
                    "/api/v2/help_center/categories", "categories"
                )
            except Exception as exc:
                log.warning(f"    Could not fetch categories for brand {prod_sub}: {exc}")
                categories = []

            # Store for CSV
            for c in categories:
                c["_brand_id"] = prod_brand_id
            self._raw_categories.extend(categories)

            if not categories:
                log.info(f"    No categories found")
                continue

            log.info(f"    Found {len(categories)} categories")

            for cat in sorted(categories, key=lambda c: c.get("position", 0)):
                pid = cat["id"]
                if self._mapped("categories", pid):
                    log.info(f"    [SKIP] Category '{cat['name']}'")
                    self.stats["categories"]["skipped"] += 1
                    continue

                if self.dry_run:
                    log.info(f"    [DRY]  Would create category '{cat['name']}'")
                    self.stats["categories"]["skipped"] += 1
                    continue

                payload = {
                    "category": {
                        "name": cat["name"],
                        "description": cat.get("description", ""),
                        "position": cat.get("position", 0),
                        "locale": cat.get("locale", "en-us"),
                    }
                }
                try:
                    # Sandbox writes use the main sandbox subdomain
                    resp = self.sand.post(
                        "/api/v2/help_center/categories",
                        json=payload,
                    )
                    if resp.status_code >= 400:
                        log.error(f"    [FAIL] Category '{cat['name']}': HTTP {resp.status_code} — {resp.text[:500]}")
                        self.stats["categories"]["failed"] += 1
                        continue
                    new_cat = resp.json()["category"]
                    self._record("categories", pid, new_cat["id"])
                    log.info(f"    [OK]   Category '{cat['name']}' → {new_cat['id']}")
                    self.stats["categories"]["created"] += 1

                    # Migrate translations for this category
                    if CONFIG["migrate_translations"]:
                        self._migrate_translations("categories", pid, new_cat["id"])

                except Exception as exc:
                    log.error(f"    [FAIL] Category '{cat['name']}': {exc}")
                    self.stats["categories"]["failed"] += 1

    # ── 3. SECTIONS ──────────────────────────────────────────────────────────

    def migrate_sections(self):
        if not CONFIG["migrate_sections"]:
            return
        log.info("═" * 60)
        log.info("PHASE 3: Migrating Sections")
        log.info("═" * 60)

        # Fetch sections per brand (brand-scoped read), write to main sandbox
        for prod_brand_id, sand_brand_id in self.mapping["brands"].items():
            subs = self._brand_subdomains.get(str(prod_brand_id), {})
            prod_sub = subs.get("prod_sub", CONFIG["prod_subdomain"])

            prod_brand_client = self.prod.for_brand(prod_sub, f"brand-{prod_brand_id}")

            try:
                all_sections = prod_brand_client.get_all(
                    "/api/v2/help_center/sections", "sections"
                )
            except Exception as exc:
                log.warning(f"  Could not fetch sections for brand {prod_sub}: {exc}")
                all_sections = []

            self._raw_sections.extend(all_sections)

            secs_by_cat = defaultdict(list)
            for sec in all_sections:
                cid = str(sec.get("category_id", ""))
                secs_by_cat[cid].append(sec)

            # Only process categories that belong to this brand
            for prod_cat_id, sand_cat_id in list(self.mapping["categories"].items()):
                sections = secs_by_cat.get(str(prod_cat_id), [])
                if not sections:
                    continue
                log.info(f"  Category {prod_cat_id} → {sand_cat_id} ({len(sections)} sections)")

                for sec in sorted(sections, key=lambda s: s.get("position", 0)):
                    pid = sec["id"]
                    if self._mapped("sections", pid):
                        log.info(f"    [SKIP] Section '{sec['name']}'")
                        self.stats["sections"]["skipped"] += 1
                        continue

                    if self.dry_run:
                        log.info(f"    [DRY]  Would create section '{sec['name']}'")
                        self.stats["sections"]["skipped"] += 1
                        continue

                    payload = {
                        "section": {
                            "name": sec["name"],
                            "description": sec.get("description", ""),
                            "position": sec.get("position", 0),
                            "locale": sec.get("locale", "en-us"),
                            "category_id": int(sand_cat_id),
                        }
                    }

                    # Handle parent_section_id for nested sections
                    if sec.get("parent_section_id"):
                        mapped_parent = self._mapped("sections", sec["parent_section_id"])
                        if mapped_parent:
                            payload["section"]["parent_section_id"] = int(mapped_parent)

                    try:
                        # Sandbox writes use the main sandbox subdomain
                        resp = self.sand.post(
                            f"/api/v2/help_center/categories/{sand_cat_id}/sections",
                            json=payload,
                        )
                        if resp.status_code >= 400:
                            log.error(f"    [FAIL] Section '{sec['name']}': HTTP {resp.status_code} — {resp.text[:500]}")
                            self.stats["sections"]["failed"] += 1
                            continue
                        new_sec = resp.json()["section"]
                        self._record("sections", pid, new_sec["id"])
                        log.info(f"    [OK]   Section '{sec['name']}' → {new_sec['id']}")
                        self.stats["sections"]["created"] += 1

                        if CONFIG["migrate_translations"]:
                            self._migrate_translations("sections", pid, new_sec["id"])

                    except Exception as exc:
                        log.error(f"    [FAIL] Section '{sec['name']}': {exc}")
                        self.stats["sections"]["failed"] += 1

    # ── 4. ARTICLES ──────────────────────────────────────────────────────────

    def migrate_articles(self):
        if not CONFIG["migrate_articles"]:
            return
        log.info("═" * 60)
        log.info("PHASE 4: Migrating Articles")
        log.info("═" * 60)

        # Zendesk requires permission_group_id and user_segment_id for article creation
        self._ensure_sandbox_permission_group()

        # Fetch articles per brand (brand-scoped read), write to main sandbox
        for prod_brand_id, sand_brand_id in self.mapping["brands"].items():
            subs = self._brand_subdomains.get(str(prod_brand_id), {})
            prod_sub = subs.get("prod_sub", CONFIG["prod_subdomain"])

            prod_brand_client = self.prod.for_brand(prod_sub, f"brand-{prod_brand_id}")

            log.info(f"  Brand {prod_brand_id} ({prod_sub})")

            try:
                all_articles = prod_brand_client.get_all(
                    "/api/v2/help_center/articles", "articles"
                )
            except Exception as exc:
                log.warning(f"    Could not fetch articles for brand {prod_sub}: {exc}")
                all_articles = []

            self._raw_articles.extend(all_articles)

            arts_by_sec = defaultdict(list)
            for art in all_articles:
                sid = str(art.get("section_id", ""))
                arts_by_sec[sid].append(art)

            for prod_sec_id, sand_sec_id in list(self.mapping["sections"].items()):
                articles = arts_by_sec.get(str(prod_sec_id), [])
                if not articles:
                    continue
                log.info(f"    Section {prod_sec_id} → {sand_sec_id} ({len(articles)} articles)")

                for art in sorted(articles, key=lambda a: a.get("position", 0)):
                    pid = art["id"]
                    if self._mapped("articles", pid):
                        log.info(f"      [SKIP] Article '{art['title']}'")
                        self.stats["articles"]["skipped"] += 1
                        continue

                    if self.dry_run:
                        log.info(f"      [DRY]  Would create article '{art['title']}'")
                        self.stats["articles"]["skipped"] += 1
                        continue

                    # Rewrite inline attachment URLs in the article body
                    body = art.get("body", "") or ""
                    if CONFIG["migrate_attachments"]:
                        body = self._migrate_inline_attachments(pid, body)

                    payload = {
                        "article": {
                            "title": art["title"],
                            "body": body,
                            "locale": art.get("locale", "en-us"),
                            "position": art.get("position", 0),
                            "promoted": art.get("promoted", False),
                            "comments_disabled": art.get("comments_disabled", False),
                            "label_names": art.get("label_names", []),
                            "draft": art.get("draft", False),
                        }
                    }

                    # Add required permission_group_id and user_segment_id
                    if self._sand_permission_group_id:
                        payload["article"]["permission_group_id"] = self._sand_permission_group_id
                    if self._sand_user_segment_id:
                        payload["article"]["user_segment_id"] = self._sand_user_segment_id

                    try:
                        # Sandbox writes use the main sandbox subdomain
                        resp = self.sand.post(
                            f"/api/v2/help_center/sections/{sand_sec_id}/articles",
                            json=payload,
                        )
                        if resp.status_code >= 400:
                            err_body = resp.text[:500]
                            log.error(
                                f"      [FAIL] Article '{art['title']}': "
                                f"HTTP {resp.status_code} — {err_body}"
                            )
                            self.stats["articles"]["failed"] += 1
                            continue
                        new_art = resp.json()["article"]
                        self._record("articles", pid, new_art["id"])
                        log.info(f"      [OK]   Article '{art['title']}' → {new_art['id']}")
                        self.stats["articles"]["created"] += 1

                        if CONFIG["migrate_translations"]:
                            self._migrate_translations("articles", pid, new_art["id"])

                    except Exception as exc:
                        log.error(f"      [FAIL] Article '{art['title']}': {exc}")
                        self.stats["articles"]["failed"] += 1

    # ── Translations ─────────────────────────────────────────────────────────

    def _migrate_translations(self, resource_type: str, prod_id, sand_id):
        """Migrate non-default translations for a resource.

        The default locale translation is created automatically when the
        resource is created, so we must skip it. We compare each
        translation's locale against the resource's source_locale (or the
        first translation marked as default) to detect it reliably.
        """
        endpoint = f"/api/v2/help_center/{resource_type}/{prod_id}/translations"
        try:
            translations = self.prod.get_all(endpoint, "translations")
        except Exception:
            return

        if not translations:
            return

        # Determine the default locale to skip — use multiple detection methods
        default_locale = None
        for tr in translations:
            if tr.get("default", False):
                default_locale = tr.get("locale", "")
                break
        # Fallback: if source_locale is available on the resource, use that
        if not default_locale and translations:
            # The first translation is usually the default
            default_locale = translations[0].get("source_locale") or translations[0].get("locale", "")

        non_default = [
            tr for tr in translations
            if tr.get("locale", "") and tr.get("locale", "") != default_locale
        ]

        if not non_default:
            return  # only the default locale exists — nothing extra to migrate

        for tr in non_default:
            locale = tr["locale"]

            tr_payload = {"translation": {"locale": locale}}
            if resource_type == "articles":
                tr_payload["translation"]["title"] = tr.get("title", "")
                tr_payload["translation"]["body"] = tr.get("body", "")
            else:
                tr_payload["translation"]["title"] = tr.get("title", "")

            try:
                resp = self.sand.post(
                    f"/api/v2/help_center/{resource_type}/{sand_id}/translations",
                    json=tr_payload,
                )
                if resp.status_code in (200, 201):
                    self.stats["translations"]["created"] += 1
                    log.debug(f"      [OK]   Translation '{locale}' for {resource_type}/{sand_id}")
                elif resp.status_code in (400, 422):
                    # 400/422 = locale already exists or not enabled in HC
                    self.stats["translations"]["skipped"] += 1
                    log.debug(f"      [SKIP] Translation '{locale}' for {resource_type}/{sand_id}: {resp.status_code}")
                else:
                    self.stats["translations"]["failed"] += 1
                    log.warning(f"      [WARN] Translation '{locale}' for {resource_type}/{sand_id}: HTTP {resp.status_code}")
            except Exception as exc:
                log.debug(f"      [WARN] Translation '{locale}' for {resource_type}/{sand_id}: {exc}")
                self.stats["translations"]["failed"] += 1

    # ── Inline attachments ───────────────────────────────────────────────────

    def _migrate_inline_attachments(self, prod_article_id, body: str) -> str:
        """
        Download inline images from production article body,
        upload them to sandbox, and rewrite the URLs.
        """
        if not body:
            return body

        # Find all image URLs pointing to the production Zendesk domain
        prod_domain = f"{CONFIG['prod_subdomain']}.zendesk.com"
        pattern = re.compile(
            rf'(src=["\'])(https?://{re.escape(prod_domain)}/hc/[^"\']+?)(["\'])',
            re.IGNORECASE,
        )

        matches = pattern.findall(body)
        if not matches:
            return body

        sand_domain = f"{CONFIG['sand_subdomain']}.zendesk.com"

        for prefix, url, suffix in matches:
            cache_key = hashlib.md5(url.encode()).hexdigest()
            if self._mapped("attachments", cache_key):
                new_url = self.mapping["attachments"][cache_key]
                body = body.replace(url, new_url)
                self.stats["attachments"]["skipped"] += 1
                continue

            try:
                # Download from production
                img_resp = self.prod.get(url.replace(f"https://{prod_domain}", ""))
                if img_resp.status_code != 200:
                    continue

                content_type = img_resp.headers.get("Content-Type", "image/png")
                ext = content_type.split("/")[-1].split(";")[0]
                filename = f"migrated_{cache_key}.{ext}"

                # Upload to sandbox article attachments
                upload_resp = self.sand.post(
                    f"/api/v2/help_center/articles/{self._mapped('articles', prod_article_id) or 'draft'}/attachments",
                    files={"file": (filename, img_resp.content, content_type)},
                    data={"inline": "true"},
                )

                if upload_resp.status_code in (200, 201):
                    att_data = upload_resp.json().get("article_attachment", {})
                    new_url = att_data.get("content_url", url.replace(prod_domain, sand_domain))
                    body = body.replace(url, new_url)
                    self._record("attachments", cache_key, new_url)
                    self.stats["attachments"]["created"] += 1
                    log.debug(f"      [OK]   Attachment {filename}")
                else:
                    # Fallback: simple domain swap
                    body = body.replace(prod_domain, sand_domain)
                    self.stats["attachments"]["failed"] += 1

            except Exception as exc:
                log.debug(f"      [WARN] Attachment migration: {exc}")
                self.stats["attachments"]["failed"] += 1

        return body

    # ── CSV Export ───────────────────────────────────────────────────────────

    def export_csv(self):
        """Save all fetched production data to local CSV files."""
        if not CONFIG["export_csv"]:
            return
        log.info("═" * 60)
        log.info("PHASE 5: Exporting to CSV")
        log.info("═" * 60)

        out_dir = Path(CONFIG["csv_output_dir"])
        out_dir.mkdir(parents=True, exist_ok=True)

        # ── Brands ───────────────────────────────────────────────────────
        brands_file = out_dir / "brands.csv"
        brand_fields = [
            "id", "name", "subdomain", "brand_url", "active",
            "has_help_center", "default", "created_at", "updated_at",
            "sandbox_id",
        ]
        with open(brands_file, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=brand_fields, extrasaction="ignore")
            writer.writeheader()
            for b in self._raw_brands:
                row = {k: b.get(k, "") for k in brand_fields}
                row["sandbox_id"] = self._mapped("brands", b["id"]) or ""
                writer.writerow(row)
        log.info(f"  [CSV] {len(self._raw_brands):4d} brands  → {brands_file}")

        # ── Categories ───────────────────────────────────────────────────
        cat_file = out_dir / "categories.csv"
        cat_fields = [
            "id", "name", "description", "locale", "position",
            "brand_id", "html_url", "outdated", "created_at", "updated_at",
            "sandbox_id",
        ]
        with open(cat_file, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=cat_fields, extrasaction="ignore")
            writer.writeheader()
            for c in self._raw_categories:
                row = {k: c.get(k, "") for k in cat_fields}
                row["brand_id"] = c.get("_brand_id", "")
                row["sandbox_id"] = self._mapped("categories", c["id"]) or ""
                writer.writerow(row)
        log.info(f"  [CSV] {len(self._raw_categories):4d} categories → {cat_file}")

        # ── Sections ─────────────────────────────────────────────────────
        sec_file = out_dir / "sections.csv"
        sec_fields = [
            "id", "name", "description", "locale", "position",
            "category_id", "parent_section_id", "html_url", "outdated",
            "sorting", "created_at", "updated_at",
            "sandbox_id",
        ]
        with open(sec_file, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=sec_fields, extrasaction="ignore")
            writer.writeheader()
            for s in self._raw_sections:
                row = {k: s.get(k, "") for k in sec_fields}
                row["sandbox_id"] = self._mapped("sections", s["id"]) or ""
                writer.writerow(row)
        log.info(f"  [CSV] {len(self._raw_sections):4d} sections   → {sec_file}")

        # ── Articles ─────────────────────────────────────────────────────
        art_file = out_dir / "articles.csv"
        art_fields = [
            "id", "title", "locale", "position", "promoted",
            "comments_disabled", "draft", "section_id", "author_id",
            "html_url", "vote_sum", "vote_count",
            "label_names", "body",
            "created_at", "updated_at", "edited_at",
            "sandbox_id",
        ]
        with open(art_file, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=art_fields, extrasaction="ignore")
            writer.writeheader()
            for a in self._raw_articles:
                row = {k: a.get(k, "") for k in art_fields}
                # Flatten label_names list into semicolon-separated string
                labels = a.get("label_names", [])
                row["label_names"] = "; ".join(labels) if isinstance(labels, list) else str(labels)
                # Truncate very long HTML bodies for CSV readability (full body still migrated)
                body_val = a.get("body", "") or ""
                row["body"] = body_val
                row["sandbox_id"] = self._mapped("articles", a["id"]) or ""
                writer.writerow(row)
        log.info(f"  [CSV] {len(self._raw_articles):4d} articles   → {art_file}")

        # ── Combined master file ─────────────────────────────────────────
        master_file = out_dir / "help_center_all.csv"
        master_fields = [
            "type", "id", "name_or_title", "parent_id", "parent_type",
            "brand_id", "locale", "position", "html_url",
            "created_at", "updated_at", "sandbox_id",
        ]
        with open(master_file, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=master_fields, extrasaction="ignore")
            writer.writeheader()

            for b in self._raw_brands:
                writer.writerow({
                    "type": "brand", "id": b["id"],
                    "name_or_title": b.get("name", ""),
                    "parent_id": "", "parent_type": "",
                    "brand_id": b["id"],
                    "locale": "", "position": "",
                    "html_url": b.get("brand_url", ""),
                    "created_at": b.get("created_at", ""),
                    "updated_at": b.get("updated_at", ""),
                    "sandbox_id": self._mapped("brands", b["id"]) or "",
                })

            for c in self._raw_categories:
                writer.writerow({
                    "type": "category", "id": c["id"],
                    "name_or_title": c.get("name", ""),
                    "parent_id": c.get("_brand_id", ""), "parent_type": "brand",
                    "brand_id": c.get("_brand_id", ""),
                    "locale": c.get("locale", ""), "position": c.get("position", ""),
                    "html_url": c.get("html_url", ""),
                    "created_at": c.get("created_at", ""),
                    "updated_at": c.get("updated_at", ""),
                    "sandbox_id": self._mapped("categories", c["id"]) or "",
                })

            for s in self._raw_sections:
                writer.writerow({
                    "type": "section", "id": s["id"],
                    "name_or_title": s.get("name", ""),
                    "parent_id": s.get("category_id", ""), "parent_type": "category",
                    "brand_id": "", "locale": s.get("locale", ""),
                    "position": s.get("position", ""),
                    "html_url": s.get("html_url", ""),
                    "created_at": s.get("created_at", ""),
                    "updated_at": s.get("updated_at", ""),
                    "sandbox_id": self._mapped("sections", s["id"]) or "",
                })

            for a in self._raw_articles:
                writer.writerow({
                    "type": "article", "id": a["id"],
                    "name_or_title": a.get("title", ""),
                    "parent_id": a.get("section_id", ""), "parent_type": "section",
                    "brand_id": "", "locale": a.get("locale", ""),
                    "position": a.get("position", ""),
                    "html_url": a.get("html_url", ""),
                    "created_at": a.get("created_at", ""),
                    "updated_at": a.get("updated_at", ""),
                    "sandbox_id": self._mapped("articles", a["id"]) or "",
                })

        total = (len(self._raw_brands) + len(self._raw_categories)
                 + len(self._raw_sections) + len(self._raw_articles))
        log.info(f"  [CSV] {total:4d} total rows → {master_file}")
        log.info(f"  CSV exports saved to: {out_dir.resolve()}")

    # ── Credential Test ─────────────────────────────────────────────────────

    def test_credentials(self) -> bool:
        """
        Validate API credentials for both production and sandbox instances.
        Tests authentication, permissions, and Help Center availability.
        Returns True if both pass, False otherwise.
        """
        log.info("═" * 60)
        log.info("PHASE 0: Testing Credentials")
        log.info("═" * 60)

        all_ok = True

        for label, client, subdomain in [
            ("Production", self.prod, CONFIG["prod_subdomain"]),
            ("Sandbox",    self.sand, CONFIG["sand_subdomain"]),
        ]:
            log.info(f"\n  ┌─ {label}: {subdomain}.zendesk.com")
            instance_ok = True

            # ── 1. Basic auth: GET /api/v2/users/me ─────────────────────
            try:
                resp = client.get("/api/v2/users/me")
                if resp.status_code == 200:
                    me = resp.json().get("user", {})
                    name = me.get("name", "Unknown")
                    role = me.get("role", "unknown")
                    email = me.get("email", "")
                    log.info(f"  │  ✓ Auth OK — {name} ({email}), role: {role}")

                    # Warn if role is too low for migration writes
                    if role not in ("admin", "owner"):
                        log.warning(
                            f"  │  ⚠ Role is '{role}'. Migration may need 'admin' "
                            f"permissions to create brands/categories/articles."
                        )
                elif resp.status_code == 401:
                    log.error(f"  │  ✗ Auth FAILED — 401 Unauthorized. Check email/token.")
                    instance_ok = False
                elif resp.status_code == 403:
                    log.error(f"  │  ✗ Auth FAILED — 403 Forbidden. Token may lack permissions.")
                    instance_ok = False
                else:
                    log.error(f"  │  ✗ Auth FAILED — HTTP {resp.status_code}: {resp.text[:200]}")
                    instance_ok = False
            except Exception as exc:
                log.error(f"  │  ✗ Auth FAILED — Connection error: {exc}")
                instance_ok = False

            # ── 2. Account info: GET /api/v2/account ────────────────────
            if instance_ok:
                try:
                    resp = client.get("/api/v2/account")
                    if resp.status_code == 200:
                        acct = resp.json().get("account", {})
                        plan = acct.get("plan_name", "unknown")
                        sandbox_flag = acct.get("sandbox", False)
                        log.info(f"  │  ✓ Account — plan: {plan}, sandbox: {sandbox_flag}")
                    else:
                        log.info(f"  │  ─ Account info unavailable (HTTP {resp.status_code})")
                except Exception:
                    log.info(f"  │  ─ Account info unavailable (skipped)")

            # ── 3. Brands access: GET /api/v2/brands ────────────────────
            if instance_ok:
                try:
                    resp = client.get("/api/v2/brands")
                    if resp.status_code == 200:
                        brands = resp.json().get("brands", [])
                        log.info(f"  │  ✓ Brands API OK — {len(brands)} brand(s) found")
                        for b in brands[:5]:
                            hc = "HC ✓" if b.get("has_help_center") else "HC ✗"
                            log.info(f"  │     • {b['name']} ({hc})")
                        if len(brands) > 5:
                            log.info(f"  │     … and {len(brands) - 5} more")
                    else:
                        log.error(f"  │  ✗ Brands API FAILED — HTTP {resp.status_code}")
                        instance_ok = False
                except Exception as exc:
                    log.error(f"  │  ✗ Brands API FAILED — {exc}")
                    instance_ok = False

            # ── 4. Help Center access: GET /api/v2/help_center/categories
            if instance_ok:
                try:
                    resp = client.get("/api/v2/help_center/categories")
                    if resp.status_code == 200:
                        cats = resp.json().get("categories", [])
                        log.info(f"  │  ✓ Help Center API OK — {len(cats)} category(ies)")
                    elif resp.status_code == 403:
                        log.warning(
                            f"  │  ⚠ Help Center API returned 403. "
                            f"Ensure Help Center is enabled for this brand."
                        )
                    elif resp.status_code == 404:
                        log.warning(
                            f"  │  ⚠ Help Center API returned 404. "
                            f"Guide / Help Center may not be activated."
                        )
                    else:
                        log.info(f"  │  ─ Help Center API returned HTTP {resp.status_code}")
                except Exception as exc:
                    log.warning(f"  │  ⚠ Help Center API check failed: {exc}")

            # ── 5. Write test (sandbox only): create + delete a draft article category
            if instance_ok and label == "Sandbox":
                try:
                    test_payload = {
                        "category": {
                            "name": "__migration_write_test__",
                            "description": "Temporary category to verify write access. Safe to delete.",
                            "locale": "en-us",
                        }
                    }
                    resp = client.post("/api/v2/help_center/categories", json=test_payload)
                    if resp.status_code in (200, 201):
                        test_id = resp.json().get("category", {}).get("id")
                        log.info(f"  │  ✓ Write test OK — created temp category {test_id}")
                        # Clean up
                        if test_id:
                            del_resp = client._request("DELETE", f"/api/v2/help_center/categories/{test_id}")
                            if del_resp.status_code in (200, 204):
                                log.info(f"  │  ✓ Cleanup OK — deleted temp category")
                            else:
                                log.warning(
                                    f"  │  ⚠ Cleanup: could not delete temp category {test_id}. "
                                    f"Delete '__migration_write_test__' manually."
                                )
                    elif resp.status_code == 403:
                        log.error(
                            f"  │  ✗ Write test FAILED — 403 Forbidden. "
                            f"Token needs write permissions for Help Center."
                        )
                        instance_ok = False
                    else:
                        log.error(f"  │  ✗ Write test FAILED — HTTP {resp.status_code}: {resp.text[:200]}")
                        instance_ok = False
                except Exception as exc:
                    log.error(f"  │  ✗ Write test FAILED — {exc}")
                    instance_ok = False

            # ── Instance verdict ────────────────────────────────────────
            if instance_ok:
                log.info(f"  └─ {label}: ALL CHECKS PASSED ✓")
            else:
                log.error(f"  └─ {label}: CHECKS FAILED ✗")
                all_ok = False

        # ── Overall verdict ─────────────────────────────────────────────
        log.info("")
        if all_ok:
            log.info("  ✅ Credential tests PASSED — ready to migrate.")
        else:
            log.error("  ❌ Credential tests FAILED — fix the issues above before migrating.")
        log.info("")

        return all_ok

    # ── Orchestrator ─────────────────────────────────────────────────────────

    def run(self):
        start = time.time()
        log.info("=" * 60)
        log.info("  ZENDESK HELP CENTER MIGRATION")
        log.info(f"  Production : {CONFIG['prod_subdomain']}.zendesk.com")
        log.info(f"  Sandbox    : {CONFIG['sand_subdomain']}.zendesk.com")
        log.info(f"  Dry Run    : {self.dry_run}")
        log.info(f"  Started    : {datetime.now().isoformat()}")
        log.info("=" * 60)

        # Phase 0: Validate credentials before doing any real work
        creds_ok = self.test_credentials()
        if not creds_ok:
            log.error("Aborting migration due to failed credential checks.")
            return
        if self._test_only:
            log.info("Test-only mode — skipping migration phases.")
            return

        try:
            self.migrate_brands()
            self.migrate_categories()
            self.migrate_sections()
            self.migrate_articles()
            self.export_csv()
        except KeyboardInterrupt:
            log.warning("\nMigration interrupted by user. Progress has been saved.")
        except Exception as exc:
            log.error(f"Migration failed: {exc}", exc_info=True)
        finally:
            self._save_mapping()

        elapsed = time.time() - start

        # ── Summary ──────────────────────────────────────────────────────
        log.info("")
        log.info("=" * 60)
        log.info("  MIGRATION SUMMARY")
        log.info("=" * 60)
        for kind, counts in self.stats.items():
            log.info(
                f"  {kind.capitalize():15s}  "
                f"Created: {counts['created']:4d}  "
                f"Skipped: {counts['skipped']:4d}  "
                f"Failed: {counts['failed']:4d}"
            )
        log.info(f"  {'─' * 54}")
        log.info(f"  Elapsed: {elapsed:.1f}s")
        log.info(f"  ID mapping saved to: {CONFIG['mapping_file']}")
        log.info(f"  Full log saved to:   {CONFIG['log_file']}")
        if CONFIG["export_csv"]:
            log.info(f"  CSV exports saved to: {CONFIG['csv_output_dir']}/")
        log.info("=" * 60)


# ╔══════════════════════════════════════════════════════════════════════════════╗
# ║  ENTRYPOINT                                                                 ║
# ╚══════════════════════════════════════════════════════════════════════════════╝

if __name__ == "__main__":
    # Quick pre-flight check
    placeholders = [v for v in [CONFIG["prod_subdomain"], CONFIG["sand_subdomain"]]
                    if "YOUR_" in v]
    if placeholders:
        print("╔════════════════════════════════════════════════════════════╗")
        print("║  SETUP REQUIRED                                           ║")
        print("╠════════════════════════════════════════════════════════════╣")
        print("║  Configure credentials at the top of this script or via   ║")
        print("║  environment variables:                                   ║")
        print("║                                                           ║")
        print("║    ZD_PROD_SUBDOMAIN   ZD_SAND_SUBDOMAIN                  ║")
        print("║    ZD_PROD_EMAIL       ZD_SAND_EMAIL                      ║")
        print("║    ZD_PROD_TOKEN       ZD_SAND_TOKEN                      ║")
        print("║                                                           ║")
        print("║  Optional:                                                ║")
        print("║    ZD_DRY_RUN=true     (preview without writing)          ║")
        print("║                                                           ║")
        print("║  Run modes:                                               ║")
        print("║    python zendesk_hc_migration.py               (full)    ║")
        print("║    python zendesk_hc_migration.py --test-only   (creds)   ║")
        print("║    python zendesk_hc_migration.py --dry-run     (preview) ║")
        print("║                                                           ║")
        print("╚════════════════════════════════════════════════════════════╝")
        sys.exit(1)

    migration = HelpCenterMigration()

    # CLI flags
    if "--test-only" in sys.argv:
        migration._test_only = True
    if "--dry-run" in sys.argv:
        migration.dry_run = True

    migration.run()

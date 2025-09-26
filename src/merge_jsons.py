#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Merge per-file resume JSONs into one master resume.
- Reads *.json recursively from INPUT_DIR
- Skips files named like master*.json
- Coerces bad data (e.g., dicts inside highlights) into strings
- Dedupes entries and sorts work experiences chronologically
- Writes master JSON to OUTPUT_MASTER
"""

from __future__ import annotations
import os
import json
import re
from pathlib import Path
from typing import Any, Dict, List, Tuple
from datetime import datetime
import difflib

try:
    # For repo-local config
    from config import INPUT_DIR, OUTPUT_DIR
except Exception:
    # Fallback defaults
    INPUT_DIR = Path("./out_resume_json")
    OUTPUT_DIR = Path("./out_resume_json")

OUTPUT_MASTER = OUTPUT_DIR / "master_resume.json"
EXCLUDE_PATTERNS = [r"^master.*\.json$", r"^merged.*\.json$"]

JSON_SCHEMA_EXAMPLE = {
  "basics": {"name": "", "label": "", "image": "", "email": "", "phone": "", "url": "", "summary": "",
    "location": {"address": "", "postalCode": "", "city": "", "countryCode": "", "region": ""},
    "profiles": [{"network": "", "username": "", "url": ""}]},
  "work": [{"name": "", "position": "", "url": "", "startDate": "", "endDate": "", "summary": "", "highlights": []}],
  "volunteer": [{"organization": "", "position": "", "url": "", "startDate": "", "endDate": "", "summary": "", "highlights": []}],
  "education": [{"institution": "", "url": "", "area": "", "studyType": "", "startDate": "", "endDate": "", "score": "", "courses": []}],
  "awards": [{"title": "", "date": "", "awarder": "", "summary": ""}],
  "certificates": [{"name": "", "date": "", "issuer": "", "url": ""}],
  "publications": [{"name": "", "publisher": "", "releaseDate": "", "url": "", "summary": ""}],
  "skills": [{"name": "", "level": "", "keywords": []}],
  "languages": [{"language": "", "fluency": ""}],
  "interests": [{"name": "", "keywords": []}],
  "references": [{"name": "", "reference": ""}],
  "projects": [{"name": "", "startDate": "", "endDate": "", "description": "", "highlights": [], "url": ""}]
}

def normalize_str(x: str) -> str:
    return re.sub(r"\s+", " ", (x or "").strip().lower())

def to_text(x: Any) -> str:
    if x is None:
        return ""
    if isinstance(x, str):
        return x
    if isinstance(x, (int, float, bool)):
        return str(x)
    if isinstance(x, list):
        return ", ".join([to_text(i) for i in x if to_text(i)])
    if isinstance(x, dict):
        if "text" in x and isinstance(x["text"], (str, int, float, bool)):
            return str(x["text"])
        for key_guess in ("bullet", "content", "value", "description", "summary", "highlight"):
            if key_guess in x and isinstance(x[key_guess], (str, int, float, bool)):
                return str(x[key_guess])
        pairs = []
        for k, v in x.items():
            vs = to_text(v)
            if vs:
                pairs.append(f"{k}: {vs}")
        return "; ".join(pairs)
    return str(x)

def coerce_list_of_strings(val: Any) -> List[str]:
    if val is None:
        return []
    if isinstance(val, str):
        s = to_text(val).strip()
        return [s] if s else []
    if isinstance(val, list):
        out = []
        for item in val:
            s = to_text(item).strip()
            if s:
                out.append(s)
        return out
    if isinstance(val, dict):
        s = to_text(val).strip()
        return [s] if s else []
    return []

def similar(a: str, b: str) -> float:
    return difflib.SequenceMatcher(a=normalize_str(a), b=normalize_str(b)).ratio()

def dedupe_list_str(items: List[Any], threshold: float = 0.92) -> List[str]:
    out: List[str] = []
    for it in items:
        s = to_text(it).strip()
        if not s:
            continue
        if any(similar(s, prev) >= threshold for prev in out):
            continue
        out.append(s)
    return out

def parse_date_iso(s: str) -> str:
    s = (s or "").strip()
    if not s:
        return ""
    for fmt in ("%Y-%m-%d", "%Y/%m/%d", "%Y.%m.%d", "%Y-%m", "%Y/%m", "%Y.%m", "%Y"):
        try:
            dt = datetime.strptime(s, fmt)
            if fmt == "%Y":
                return f"{dt.year:04d}-01-01"
            if fmt in ("%Y-%m", "%Y/%m", "%Y.%m"):
                return f"{dt.year:04d}-{dt.month:02d}-01"
            return dt.strftime("%Y-%m-%d")
        except Exception:
            pass
    m = re.search(r"(19|20)\d{2}", s)
    if m:
        return f"{m.group(0)}-01-01"
    return s

from typing import Tuple
def merge_date_range(a_start: str, a_end: str, b_start: str, b_end: str) -> Tuple[str, str]:
    def to_dt(s):
        s = parse_date_iso(s)
        try:
            return datetime.strptime(s, "%Y-%m-%d").date() if s else None
        except Exception:
            return None
    asd, aed, bsd, bed = map(to_dt, [a_start, a_end, b_start, b_end])
    starts = [d for d in (asd, bsd) if d]
    start = min(starts) if starts else None
    ends = [d for d in (aed, bed) if d]
    end = max(ends) if ends else None
    return (start.isoformat() if start else ""), (end.isoformat() if end else "")

def merge_work_entries(a: Dict[str, Any], b: Dict[str, Any]) -> Dict[str, Any]:
    start, end = merge_date_range(a.get("startDate",""), a.get("endDate",""), b.get("startDate",""), b.get("endDate",""))
    highlights = dedupe_list_str(coerce_list_of_strings(a.get("highlights")) + coerce_list_of_strings(b.get("highlights")))
    summary = " ".join(dedupe_list_str([to_text(a.get("summary","")), to_text(b.get("summary",""))], threshold=0.98)).strip()
    url = a.get("url") or b.get("url") or ""
    return {
        "name": a.get("name") or b.get("name") or "",
        "position": a.get("position") or b.get("position") or "",
        "url": url,
        "startDate": start,
        "endDate": end,
        "summary": summary,
        "highlights": highlights
    }

def merge_sections_list_of_objs(section_lists: List[List[Dict[str, Any]]], key_fields: List[str]) -> List[Dict[str, Any]]:
    bucket: Dict[str, Dict[str, Any]] = {}
    def makekey(o: Dict[str, Any]) -> str:
        parts = [normalize_str(to_text(o.get(k,""))) for k in key_fields]
        return " | ".join(parts)
    for section in section_lists:
        for obj in section or []:
            for fld in ("highlights", "courses", "keywords"):
                if fld in obj:
                    obj[fld] = dedupe_list_str(coerce_list_of_strings(obj.get(fld)))
            for d in ("date","startDate","endDate","releaseDate"):
                if d in obj:
                    obj[d] = parse_date_iso(obj.get(d,""))
            k = makekey(obj)
            if not k.strip(" |"):
                continue
            if k not in bucket:
                bucket[k] = obj
            else:
                existing = bucket[k]
                for fld in ("highlights", "courses", "keywords"):
                    if fld in obj:
                        existing[fld] = dedupe_list_str(coerce_list_of_strings(existing.get(fld)) + coerce_list_of_strings(obj.get(fld)))
                for fld, val in obj.items():
                    if isinstance(val, str) and val and not to_text(existing.get(fld,"")).strip():
                        existing[fld] = val
    return list(bucket.values())

def merge_skills_lists(skills_lists: List[List[Dict[str, Any]]]) -> List[Dict[str, Any]]:
    merged: Dict[str, Dict[str, Any]] = {}
    level_rank = {"":0, "beginner":1, "intermediate":2, "advanced":3, "expert":4, "master":5}
    for skills in skills_lists:
        for s in skills or []:
            name = to_text(s.get("name","")).strip()
            if not name:
                continue
            key = normalize_str(name)
            entry = merged.get(key, {"name": name, "level": "", "keywords": []})
            new_level = to_text(s.get("level","")).strip().lower()
            cur_level = entry.get("level","").lower()
            if level_rank.get(new_level,0) > level_rank.get(cur_level,0):
                entry["level"] = new_level
            kws = dedupe_list_str(coerce_list_of_strings(s.get("keywords")))
            entry["keywords"] = dedupe_list_str(entry.get("keywords", []) + kws)
            entry["name"] = name or entry["name"]
            merged[key] = entry
    return sorted(merged.values(), key=lambda x: normalize_str(x["name"]))

def merge_all_resumes(resumes: List[Dict[str, Any]]) -> Dict[str, Any]:
    def basics_score(b):
        if not b: return 0
        fields = ["name","label","email","phone","url","summary"]
        return sum(1 for f in fields if to_text(b.get(f,"")).strip())
    chosen_basics = max((r.get("basics",{}) for r in resumes), key=basics_score, default={}) or {}
    profiles = []
    for r in resumes:
        profiles += r.get("basics",{}).get("profiles") or []
    profiles_merged = merge_sections_list_of_objs([profiles], ["network","username"])
    if chosen_basics:
        chosen_basics["profiles"] = profiles_merged

    all_work = [w for r in resumes for w in (r.get("work") or [])]
    for w in all_work:
        w["startDate"] = parse_date_iso(w.get("startDate",""))
        w["endDate"] = parse_date_iso(w.get("endDate",""))
        w["highlights"] = dedupe_list_str(coerce_list_of_strings(w.get("highlights")))
        w["summary"] = to_text(w.get("summary",""))
    keymap: Dict[str, Dict[str, Any]] = {}
    def wkey(w):
        c = normalize_str(to_text(w.get("name","")))
        p = normalize_str(to_text(w.get("position","")))
        return f"{c}||{p}"
    for w in all_work:
        k = wkey(w)
        if not k.strip("|"):
            k = f"{normalize_str(to_text(w.get('position','')))}||{w.get('startDate','')}||{w.get('endDate','')}"
        if k not in keymap:
            keymap[k] = w
        else:
            keymap[k] = merge_work_entries(keymap[k], w)

    merged_work = list(keymap.values())

    def parse_iso(s: str) -> datetime:
        try:
            return datetime.strptime(s, "%Y-%m-%d")
        except Exception:
            return datetime(1900,1,1)
    merged_work.sort(key=lambda w: (parse_iso(w.get("startDate","")), parse_iso(w.get("endDate",""))), reverse=True)

    merged_volunteer    = merge_sections_list_of_objs([r.get("volunteer") or [] for r in resumes], ["organization","position","startDate","endDate"])
    merged_education    = merge_sections_list_of_objs([r.get("education") or [] for r in resumes], ["institution","area","studyType","startDate","endDate"])
    merged_awards       = merge_sections_list_of_objs([r.get("awards") or [] for r in resumes], ["title","awarder","date"])
    merged_certificates = merge_sections_list_of_objs([r.get("certificates") or [] for r in resumes], ["name","issuer","date"])
    merged_publications = merge_sections_list_of_objs([r.get("publications") or [] for r in resumes], ["name","publisher","releaseDate"])
    merged_languages    = merge_sections_list_of_objs([r.get("languages") or [] for r in resumes], ["language","fluency"])
    merged_interests    = merge_sections_list_of_objs([r.get("interests") or [] for r in resumes], ["name"])
    merged_references   = merge_sections_list_of_objs([r.get("references") or [] for r in resumes], ["name"])
    merged_projects     = merge_sections_list_of_objs([r.get("projects") or [] for r in resumes], ["name","startDate","endDate","url"])

    merged_skills = merge_skills_lists([r.get("skills") or [] for r in resumes])

    master = {
        "basics": chosen_basics or JSON_SCHEMA_EXAMPLE["basics"],
        "work": merged_work,
        "volunteer": merged_volunteer,
        "education": merged_education,
        "awards": merged_awards,
        "certificates": merged_certificates,
        "publications": merged_publications,
        "skills": merged_skills,
        "languages": merged_languages,
        "interests": merged_interests,
        "references": merged_references,
        "projects": merged_projects
    }
    return master

def should_exclude(name: str) -> bool:
    for pat in EXCLUDE_PATTERNS:
        if re.search(pat, name, flags=re.IGNORECASE):
            return True
    return False

def load_all_jsons(root: Path) -> List[Dict[str, Any]]:
    resumes: List[Dict[str, Any]] = []
    for p in root.rglob("*.json"):
        if should_exclude(p.name):
            continue
        try:
            with p.open("r", encoding="utf-8") as fh:
                data = json.load(fh)
            if isinstance(data, dict):
                resumes.append(data)
            else:
                print(f"[!] Skipping non-dict JSON: {p}")
        except Exception as e:
            print(f"[!] Failed to load {p}: {e}")
    return resumes

def main():
    in_dir = Path(INPUT_DIR).expanduser().resolve()
    out_dir = Path(OUTPUT_DIR).expanduser().resolve()
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = (out_dir / "master_resume.json").resolve()

    resumes = load_all_jsons(in_dir)
    if not resumes:
        print(f"[!] No JSON resumes found in {in_dir}")
        return

    master = merge_all_resumes(resumes)
    with out_path.open("w", encoding="utf-8") as fh:
        json.dump(master, fh, ensure_ascii=False, indent=2)
    print(f"[✓] Master resume written to: {out_path}")

if __name__ == "__main__":
    main()

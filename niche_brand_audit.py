#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
niche_brand_audit.py
— Проходит по ВСЕМ брендам из brands_catalog.canonical_list(country)
— Ищет кандидатов в Google Play (top-N), по каждому тянет installs/day из AppstoreSpy
— Определяет, забанен ли кандидат (по данным AppstoreSpy), и всё равно допускает его к выбору
— В качестве «конкурента» берём кандидата с максимальными installs/day (если все None — первого)
— Помечаем «Юзаный» по тайтлам моих приложений в Keyapp

Выходной CSV: niche_competitors_keyapp.csv
Колонки:
  ключ, конкурент, конкурент_url, конкурент_app_id, Юзаный, страна,
  «инстайлы в день», «конкурент в бане», конкуренты_инсталлы
где конкуренты_инсталлы = "Title::installs::Да/Нет; ..."
"""

import argparse
import time
import re
import unicodedata
from typing import List, Dict, Any, Optional

import pandas as pd
import requests
from google_play_scraper import search as gp_search

# каталог стран/брендов
from brands_catalog import (
    get_supported_countries,
    get_country_title,
    get_country_language,
    canonical_list,
)

# Keyapp API
DEFAULT_BASE_URL = "https://keyapp.top/api/v2"
DEFAULT_PATHS = {"apps": "/apps"}

from pathlib import Path
import json, hashlib, datetime as dt

CACHE_DIR = Path(".cache")
CACHE_DIR.mkdir(exist_ok=True)
def _now(): return dt.datetime.utcnow()

def _read_cache(name):
    p = CACHE_DIR / f"{name}.json"
    if p.exists():
        try: return json.loads(p.read_text("utf-8"))
        except: return {}
    return {}

def _write_cache(name, data):
    (CACHE_DIR / f"{name}.json").write_text(json.dumps(data), encoding="utf-8")

# кэши:
play_cache = _read_cache("play_search")
aspy_cache = _read_cache("aspy_meta")

def _expired(ts_iso: str, ttl_days: int) -> bool:
    try:
        ts = dt.datetime.fromisoformat(ts_iso)
        return (_now() - ts).days >= ttl_days
    except:
        return True

def play_search_cached(query, lang, cc, topn, ttl_days):
    key = hashlib.md5(f"{query}|{lang}|{cc}|{topn}".encode()).hexdigest()
    rec = play_cache.get(key)
    if isinstance(rec, dict) and not _expired(rec.get("ts", ""), ttl_days):
        data = rec.get("data")
        if isinstance(data, list) and data:
            return data
        # пустые ответы ([], None) не кэшируем — мог быть сетевой сбой
        play_cache.pop(key, None)

    data = play_search_candidates(query, lang, cc, topn)
    if data:
        play_cache[key] = {"ts": _now().isoformat(), "data": data}
    else:
        play_cache.pop(key, None)
    _write_cache("play_search", play_cache)
    return data

def aspy_enrich_cached(app_id, session, ttl_days):
    rec = aspy_cache.get(app_id)
    if isinstance(rec, dict) and not _expired(rec.get("ts", ""), ttl_days):
        data = rec.get("data")
        if isinstance(data, dict) and data:
            return data
        # некорректная запись — пересчитаем
        aspy_cache.pop(app_id, None)

    data = aspy_enrich(app_id, session)
    if data:
        aspy_cache[app_id] = {"ts": _now().isoformat(), "data": data}
    else:
        aspy_cache.pop(app_id, None)
    _write_cache("aspy_meta", aspy_cache)
    return data

# AppstoreSpy API
ASPY_BASE = "https://api.appstorespy.com/v1"

def _headers_keyapp(token: str) -> Dict[str,str]:
    return {"Authorization": f"Bearer {token}", "Accept": "application/json"}

def _headers_aspy(api_key: str) -> Dict[str,str]:
    return {"Authorization": f"Bearer {api_key}", "Accept": "application/json"}

def normalize_text(s: str) -> str:
    s = (s or "").strip().lower()
    s = ''.join(c for c in unicodedata.normalize('NFKD', s) if not unicodedata.combining(c))
    return re.sub(r'[^a-z0-9]+','',s)

import re, unicodedata

def _normalize_tokens(s: str) -> list[str]:
    s = (s or "").lower().strip()
    s = ''.join(c for c in unicodedata.normalize('NFKD', s) if not unicodedata.combining(c))
    # токены: буквы/цифры, длиной >= 3
    return [t for t in re.findall(r"[a-z0-9]+", s) if len(t) >= 3]

def any_brand_in_title(brand: str, titles: list[str]) -> bool:
    """
    Строгая проверка: разбиваем бренд и тайтл на токены и ищем пересечение токенов.
    Примеры: 'LV BET' -> {'lv','bet'}; 'bet365' -> {'bet365'}.
    Совпадение по хотя бы одному токену длиной >= 4 либо по полному тегу (например 'bet365').
    """
    b_tokens = _normalize_tokens(brand)
    # если бренд односложный из 3 букв (типа 'PIN'), не считаем — слишком много ложных попаданий
    if not b_tokens or (len(b_tokens) == 1 and len(b_tokens[0]) < 4):
        return False

    for t in titles:
        t_tokens = set(_normalize_tokens(t))
        if not t_tokens:
            continue
        # полное совпадение токена (например 'bet365')
        if any(bt in t_tokens for bt in b_tokens if len(bt) >= 4):
            return True
        # два и более совпавших токена для составных брендов (например {'lv','bet'})
        inter = t_tokens.intersection(b_tokens)
        if len(b_tokens) >= 2 and len(inter) >= 2:
            return True
    return False

def keyapp_fetch_app_titles(base_url: str, token: str) -> List[str]:
    url = base_url.rstrip("/") + DEFAULT_PATHS["apps"]
    try:
        r = requests.get(url, headers=_headers_keyapp(token), timeout=30)
        r.raise_for_status()
        data = r.json()
    except Exception:
        return []
    arr = data if isinstance(data, list) else (data.get('apps') or data.get('data') or [])
    titles = []
    for it in arr:
        t = it.get('title') or it.get('name') or it.get('app_title') or it.get('store_title') or it.get('appName')
        if t:
            titles.append(str(t))
    return titles

# ---------- Google Play ----------

def play_search_candidates(keyword: str, lang: str, country_cc: str, topn: int, retries: int = 2, pause: float = 0.5) -> List[Dict[str, Any]]:
    out = []
    res = []
    for attempt in range(1, retries+1):
        try:
            res = gp_search(keyword, n=topn, lang=lang, country=country_cc.upper())
        except Exception:
            time.sleep(pause * attempt)
            try:
                res = gp_search(keyword, n=topn)  # fallback без локали
            except Exception:
                res = []
        if res:
            break
    for item in (res or []):
        app_id = item.get('appId')
        title = str(item.get('title','')).strip()
        if app_id:
            out.append({
                "appId": app_id,
                "title": title,
                "url": f"https://play.google.com/store/apps/details?id={app_id}"
            })
    return out

# ---------- AppstoreSpy helpers ----------

def _aspy_request(
    session: requests.Session,
    method: str,
    endpoint: str,
    *,
    params: Optional[Dict[str, Any]] = None,
    payload: Optional[Dict[str, Any]] = None,
) -> Any:
    """Helper for AppstoreSpy requests (v1 API, bearer auth already on the session)."""

    url = f"{ASPY_BASE}{endpoint}"
    try:
        r = session.request(method, url, params=params, json=payload, timeout=30)
    except Exception:
        return {}

    if r.status_code in {401, 403}:
        # неверный ключ — смысла продолжать нет
        return {}

    if r.status_code in {404, 422}:
        # нет данных по приложению — возвращаем пусто
        return {}

    if r.status_code == 429:
        # превышение лимита — не стопорим пайплайн, просто отдаём пусто
        return {}

    if not r.ok:
        return {}

    try:
        data = r.json()
    except ValueError:
        return {}

    # Некоторые эндпоинты (installs_daily) возвращают список, остальные — dict
    return data or {}

def _extract_daily_installs_any(data: Dict[str, Any]) -> Optional[float]:
    # Пытаемся вынуть daily installs из разных возможных мест
    candidates = [
        ("ipd",),
        ("downloads_daily",),
        ("installs_daily",),
        ("daily_installs",),
        ("est_installs_per_day",),
        ("installs_per_day",),
        ("metrics","daily_installs"),
        ("summary","daily_installs"),
        ("downloads","daily"),
    ]
    for path in candidates:
        node = data
        ok = True
        for k in path:
            if isinstance(node, dict) and k in node:
                node = node[k]
            else:
                ok = False
                break
        if ok and node is not None:
            try:
                return float(node)
            except Exception:
                try:
                    return float(node[-1]) if isinstance(node, list) and node else None
                except Exception:
                    pass
    return None

def _extract_daily_series_any(data: Dict[str, Any]) -> List[float]:
    if isinstance(data, list):
        seq = data
    else:
        seq = None
        series_keys = ["daily_installs", "installs_daily", "downloads_daily", "installs_per_day"]
        for k in series_keys:
            if k in data:
                seq = data[k]
                break

    if not isinstance(seq, list):
        return []

    out = []
    for v in seq:
        if isinstance(v, dict):
            v = v.get("ipd") or v.get("value") or v.get("v") or v.get("count")
        try:
            out.append(float(v))
        except Exception:
            continue
    return out

def _extract_banned_flag(data: Dict[str, Any]) -> Optional[bool]:
    """
    Пытаемся понять, забанено ли приложение.
    Типичные варианты полей в ответах AppstoreSpy:
      - status: 'removed'|'suspended'|'banned'|'unpublished'|'deleted'
      - availability: 'not available'
      - is_published / is_available (boolean)
    """
    # простые флаги
    for k in ("is_banned","banned","removed","suspended","unpublished","deleted"):
        v = data.get(k)
        if isinstance(v, bool):
            if v: return True
    # новые ключи API v1
    for k in ("published", "listing_visible"):
        v = data.get(k)
        if isinstance(v, bool):
            return not v
    # статусные строки
    status = str(data.get("status","")).lower()
    if any(x in status for x in ("banned","removed","suspended","unpublished","deleted","not available","terminated")):
        return True
    # вложенные summary/metrics
    for top in ("summary","metrics","app","details"):
        if top in data and isinstance(data[top], dict):
            v = _extract_banned_flag(data[top])
            if v is not None:
                return v
    # явные boolean
    for k in ("is_published","is_available"):
        v = data.get(k)
        if isinstance(v, bool):
            return not v
    return None

def aspy_enrich(app_id: str, session: requests.Session) -> Dict[str, Any]:
    """
    Возвращает {'daily': float|None, 'banned': bool|None}
    Использует три эндпоинта: summary, app, trends
    """
    out = {"daily": None, "banned": None}

    # 1) детальная карточка приложения
    data = _aspy_request(session, "GET", f"/play/apps/{app_id}")
    if isinstance(data, dict) and data:
        if out["daily"] is None:
            out["daily"] = _extract_daily_installs_any(data)
        b = _extract_banned_flag(data)
        if b is not None:
            out["banned"] = bool(b)

    # 2) daily installs (по датам)
    if out["daily"] is None:
        data = _aspy_request(session, "GET", f"/play/apps/{app_id}/installs_daily")
        if isinstance(data, list) and data:
            try:
                last = next(
                    (float(item.get("ipd")) for item in reversed(data) if item.get("ipd") is not None),
                    None,
                )
            except Exception:
                last = None
            if last is not None:
                out["daily"] = last

    # 3) fallback: агрегированный поиск по bundle
    if out["daily"] is None or out["banned"] is None:
        payload = {"filter": {"bundle": app_id}, "limit": 1}
        data = _aspy_request(session, "POST", "/play/apps/query", payload=payload)
        if isinstance(data, dict):
            items = data.get("data") or data.get("apps") or data.get("items")
        elif isinstance(data, list):
            items = data
        else:
            items = []
        if items:
            node = items[0]
            if out["daily"] is None:
                out["daily"] = _extract_daily_installs_any(node)
            if out["banned"] is None:
                b = _extract_banned_flag(node)
                if b is not None:
                    out["banned"] = bool(b)

    return out

# ---------- основной цикл по стране ----------

def audit_country_all_brands(
    country_code: str,
    keyapp_titles: List[str],
    aspy_key: Optional[str],
    topn: int,
    play_sleep: float,
    aspy_sleep: float,
    cache_ttl_days: int = 3,
) -> pd.DataFrame:
    brands = canonical_list(country_code)
    lang = get_country_language(country_code)
    country_title = get_country_title(country_code)

    sess = None
    if aspy_key:
        sess = requests.Session()
        sess.headers.update(_headers_aspy(aspy_key))

    rows = []
    for kw in brands:
        # 1) кандидаты из Google Play
        candidates = play_search_cached(kw, lang, country_code, topn, cache_ttl_days)
        time.sleep(play_sleep)

        # 2) для каждого — тянем installs/day + banned
        enriched = []
        for c in candidates:
            daily = None
            banned = None
            if sess is not None:
                meta = aspy_enrich_cached(c["appId"], sess, cache_ttl_days)
                daily = meta.get("daily")
                banned = meta.get("banned")
                time.sleep(aspy_sleep)
            enriched.append({**c, "daily": daily, "banned": banned})

        # 3) выбираем «жирного» по installs/day (если все None — первый)
        top = None
        if enriched:
            non_null = [x for x in enriched if x["daily"] is not None]
            top = (max(non_null, key=lambda x: x["daily"]) if non_null else enriched[0])

        comp_title = top["title"] if top else ""
        comp_url = top["url"] if top else ""
        comp_daily = int(top["daily"]) if (top and isinstance(top["daily"], (int, float))) else ""
        comp_banned = top.get("banned") if top else None
        comp_banned = bool(comp_banned) if isinstance(comp_banned, bool) else None
        comp_app_id = top.get("appId") if top else ""

        # 4) сводка кандидатов: "Title::installs::banned"
        bundle = "-"
        if enriched:
            parts = []
            for ci in enriched:
                di = (int(ci["daily"]) if isinstance(ci["daily"], (int, float)) else "-")
                bn_flag = ci.get("banned")
                if isinstance(bn_flag, bool):
                    bn = "Да" if bn_flag else "Нет"
                else:
                    bn = "?"
                parts.append(f"{ci['title']}::{di}::{bn}")
            bundle = "; ".join(parts)

        rows.append({
            "ключ": kw,
            "конкурент": comp_title,
            "конкурент_url": comp_url,
            "конкурент_app_id": comp_app_id,
            "Юзаный": "Да" if any_brand_in_title(kw, keyapp_titles) else "Нет",
            "страна": country_title,
            "инстайлы в день": comp_daily,
            "конкурент в бане": ("Да" if comp_banned is True else "Нет" if comp_banned is False else ""),
            "конкуренты_инсталлы": bundle
        })

    return pd.DataFrame(rows)

def main():
    ap = argparse.ArgumentParser(
        description="All-brands audit → competitor with max AppstoreSpy installs/day (+banned flag) + Keyapp 'Юзаный'"
    )
    ap.add_argument("--api-key", required=True, help="Keyapp API token (Bearer)")
    ap.add_argument("--base-url", default=DEFAULT_BASE_URL)
    ap.add_argument("--country", choices=get_supported_countries() + ["all"], default="all")
    ap.add_argument("--topn", type=int, default=10, help="Сколько результатов Play смотреть на бренд")
    ap.add_argument("--play-sleep", type=float, default=0.2, help="Пауза между поисками Play (сек.)")
    ap.add_argument("--aspy-sleep", type=float, default=0.15, help="Пауза между запросами AppstoreSpy (сек.)")
    ap.add_argument("--appstorespy-key", default=None, help="AppstoreSpy API key (Bearer)")
    ap.add_argument("--out", default="niche_competitors_keyapp.csv")
    ap.add_argument("--cache-ttl-days", type=int, default=3,
                    help="TTL кэша для AppstoreSpy/Play (дни)")
    args = ap.parse_args()

    titles = keyapp_fetch_app_titles(args.base_url, args.api_key)
    countries = get_supported_countries() if args.country == "all" else [args.country]

    frames = []
    for cc in countries:
        frames.append(
            audit_country_all_brands(
                country_code=cc,
                keyapp_titles=titles,
                aspy_key=args.appstorespy_key,
                topn=args.topn,
                play_sleep=args.play_sleep,
                aspy_sleep=args.aspy_sleep,
                cache_ttl_days=args.cache_ttl_days,
            )
        )

    final = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame(
        columns=["ключ","конкурент","Юзаный","страна","installs_daily","конкурент_забанен","конкуренты_инсталлы"]
    )
    final.to_csv(args.out, index=False, encoding='utf-8-sig')
    print(f"Saved: {args.out}")

if __name__ == "__main__":
    main()

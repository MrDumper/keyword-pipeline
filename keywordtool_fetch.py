#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
keywordtool_fetch.py
— Тянет search volume из KeywordTool по брендам.
— В запрос шлёт ВСЕ варианты (из brands_catalog), а в CSV пишет ТОЛЬКО каноны (агрегация по MAX).
— Страны, языки, location_id и списки брендов берутся из brands_catalog.py.
"""

import argparse
import csv
import json
import time
from typing import Dict, List, Iterable, Tuple, Optional

import requests

from brands_catalog import (
    get_supported_countries,
    get_country_language,
    get_country_location_id,
    get_country_title,
    canonical_list,
    all_variants_for_country,
    canonicalize,
)

ENDPOINT = "https://api.keywordtool.io/v2/search/volume/google"


# ------------------------ утилиты ------------------------

def chunked(iterable: List[str], n: int) -> Iterable[List[str]]:
    for i in range(0, len(iterable), n):
        yield iterable[i:i + n]


def request_keywordtool(
    apikey: str,
    metrics_location: int,
    metrics_language: str,
    keywords: List[str],
    metrics_network: str = "googlesearchnetwork",
    retries: int = 5,
    backoff: float = 1.5,
    timeout: int = 60,
) -> Dict:
    """
    Важные поля:
      - 'keyword': массив запросов
      - 'metrics_location': [Google Ads Location ID]
      - 'metrics_language': [код языка]
      - 'metrics_network': 'googlesearch' | 'googlesearchnetwork'
    """
    payload = {
        "apikey": apikey,
        "keyword": keywords,
        "metrics_location": [metrics_location],
        "metrics_language": [metrics_language],
        "metrics_network": metrics_network,
        "output": "json",
    }

    attempt = 0
    while True:
        attempt += 1
        try:
            resp = requests.post(ENDPOINT, json=payload, timeout=timeout)
            if resp.status_code == 200:
                return resp.json()

            # попытка прочитать тело для диагностики
            try:
                body = resp.json()
            except Exception:
                body = resp.text

            print(f"[API error] status={resp.status_code}, attempt={attempt}, body={str(body)[:500]}")

            if resp.status_code in (429, 500, 502, 503, 504) and attempt < retries:
                time.sleep(backoff ** attempt)
                continue

            resp.raise_for_status()

        except requests.RequestException as e:
            if attempt < retries:
                time.sleep(backoff ** attempt)
                continue
            raise e


def _coerce_num(x):
    try:
        return int(x)
    except Exception:
        try:
            return float(x)
        except Exception:
            return None


def flatten_results(data: Dict, country_code: str, language_code: str) -> List[Dict]:
    """
    Универсальная нормализация ответа KeywordTool.
    Поддерживает варианты:
      - {"results": {"kw": {...}, ...}}
      - {"results": [ {...}, ... ]}
      - {"keywords": {"kw": {...}}}
      - {"data": {...}}

    Маппинг:
      volume -> search_volume
      cmp    -> competition
      m1..m12 -> trend (список)
    """
    results = data.get("results") or data.get("data") or data.get("keywords") or {}
    items: List[Dict] = []

    if isinstance(results, list):
        # редкий случай: список объектов с ключами
        items = [obj for obj in results if isinstance(obj, dict)]
    elif isinstance(results, dict) and "keywords" in results:
        for kw, metrics in (results["keywords"] or {}).items():
            m = {"string": kw}
            if isinstance(metrics, dict):
                m.update(metrics)
            items.append(m)
    elif isinstance(results, dict):
        # обычный: словарь kw -> metrics
        for kw, metrics in results.items():
            if not isinstance(metrics, dict):
                continue
            # пропускаем служебные ключи (напр. "status") без метрик
            if not any(k in metrics for k in ("volume", "search_volume", "m1", "string")):
                continue
            m = {"string": metrics.get("string", kw)}
            m.update(metrics)
            items.append(m)

    out: List[Dict] = []
    for it in items:
        kw = it.get("string", "")
        vol = it.get("search_volume", it.get("volume"))
        cpc = it.get("cpc")
        cmp_ = it.get("competition", it.get("cmp"))

        trend_vals = []
        any_month = False
        for i in range(1, 13):
            key = f"m{i}"
            if key in it:
                any_month = True
                trend_vals.append(_coerce_num(it.get(key)))
        if not any_month:
            # у некоторых аккаунтов приходит "trend": число
            t = it.get("trend")
            if t is not None:
                trend_vals = [_coerce_num(t)]

        out.append({
            "keyword": kw,
            "country": country_code,
            "language": language_code,
            "search_volume": _coerce_num(vol),
            "cpc": _coerce_num(cpc),
            "competition": _coerce_num(cmp_),
            "trend": json.dumps(trend_vals, ensure_ascii=False),
        })

    return out


def write_csv(path: str, rows: List[Dict]):
    fields = ["keyword", "country", "language", "search_volume", "cpc", "competition", "trend"]
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        w.writerows(rows)


def summarize(rows: List[Dict], title: str, topn: int = 10):
    def sv(x):
        try:
            return int(x.get("search_volume") or 0)
        except Exception:
            return 0

    top = sorted(rows, key=sv, reverse=True)[:topn]
    print(f"\n=== {title}: топ {topn} по объёму ===")
    for i, r in enumerate(top, 1):
        print(f"{i:>2}. {r['keyword']} — {r.get('search_volume')}")


def aggregate_to_canonical(rows: List[Dict], country_code: str) -> List[Dict]:
    """
    Сворачиваем вариации в канон по MAX(search_volume).
    В CSV уходит один ряд на канон.
    """
    agg: Dict[str, Dict] = {}
    for r in rows:
        variant = r.get("keyword", "")
        canon = canonicalize(country_code, variant)
        if not canon:
            # вариант не привязался к канону — игнорируем
            continue

        sv = r.get("search_volume") or 0
        if canon not in agg:
            agg[canon] = {
                "keyword": canon,
                "country": country_code,
                "language": r.get("language"),
                "search_volume": sv,
                "cpc": r.get("cpc"),
                "competition": r.get("competition"),
                "trend": r.get("trend"),
            }
        else:
            if (sv or 0) > (agg[canon]["search_volume"] or 0):
                # переносим метрики с лучшего (по объёму) варианта
                agg[canon]["search_volume"] = sv
                agg[canon]["cpc"] = r.get("cpc")
                agg[canon]["competition"] = r.get("competition")
                agg[canon]["trend"] = r.get("trend")

    return list(agg.values())


def _result_filename(code: str) -> str:
    return f"{code}_results.csv" if code not in ("br", "pl") else f"{code}_results.csv"


# ------------------------ health-check ------------------------

def health_check(api_key: str, countries: List[str]):
    """
    Мини-проверка: делает по одному тесту на страну + общий контроль.
    Не критично для работы, можно отключить --skip-health.
    """
    print("\n--- HEALTH CHECK ---")
    # общий тест
    try:
        data = request_keywordtool(
            apikey=api_key,
            metrics_location=get_country_location_id(countries[0]),
            metrics_language=get_country_language(countries[0]),
            keywords=["google", "facebook"],
            metrics_network="googlesearchnetwork",
            retries=2,
            backoff=1.3,
            timeout=30,
        )
        blk = data.get("results") or data.get("data") or data.get("keywords") or {}
        print("[global] ok, sample parsed keys:", (list(blk)[:3] if isinstance(blk, dict) else "list"))
    except Exception as e:
        print("[global] failed:", e)

    # по стране
    for c in countries:
        try:
            sample = canonical_list(c)[:3] or ["test"]
            data = request_keywordtool(
                apikey=api_key,
                metrics_location=get_country_location_id(c),
                metrics_language=get_country_language(c),
                keywords=sample,
                metrics_network="googlesearchnetwork",
                retries=2,
                backoff=1.3,
                timeout=30,
            )
            blk = data.get("results") or data.get("data") or data.get("keywords") or {}
            print(f"[{c}] ok, sample size:", (len(blk) if isinstance(blk, dict) else len(blk)))
        except Exception as e:
            print(f"[{c}] failed:", e)


# ------------------------ основной сценарий ------------------------

def fetch_for_country(
    api_key: str,
    country_code: str,
    only_canon: bool,
    batch_size: int,
    metrics_network: str,
    sleep_between: float,
    retries: int,
    backoff: float,
    timeout: int,
) -> List[Dict]:
    """
    Возвращает канонически агрегированные строки для страны.
    Также пишет country_results.csv на диск.
    """
    lang = get_country_language(country_code)
    loc_id = get_country_location_id(country_code)
    title = get_country_title(country_code)

    keywords = canonical_list(country_code) if only_canon else all_variants_for_country(country_code)
    print(f"{country_code.upper()} keywords total (to query): {len(keywords)}")

    rows_variants: List[Dict] = []
    for chunk in chunked(keywords, batch_size):
        data = request_keywordtool(
            apikey=api_key,
            metrics_location=loc_id,
            metrics_language=lang,
            keywords=chunk,
            metrics_network=metrics_network,
            retries=retries,
            backoff=backoff,
            timeout=timeout,
        )
        rows_variants.extend(flatten_results(data, country_code, lang))
        time.sleep(sleep_between)

    rows_canon = aggregate_to_canonical(rows_variants, country_code)
    out_path = _result_filename(country_code)
    write_csv(out_path, rows_canon)
    summarize(rows_canon, f"{title} ({lang})")
    print(f"Saved: {out_path}")
    return rows_canon


def main():
    parser = argparse.ArgumentParser(
        description="Fetch KeywordTool volumes (canonical CSVs) — country-aware & catalog-driven"
    )
    parser.add_argument("--api-key", required=True, help="KeywordTool API key")
    parser.add_argument(
        "--country",
        choices=get_supported_countries() + ["all"],
        default="all",
        help="Страна для выгрузки (или all)",
    )
    parser.add_argument("--batch-size", type=int, default=700, help="Keywords per request (1–1000)")
    parser.add_argument("--metrics-network", choices=["googlesearch", "googlesearchnetwork"],
                        default="googlesearchnetwork")
    parser.add_argument("--no-variants", action="store_true",
                        help="Шить только каноны (без вариантов) для указанной страны(стран)")
    parser.add_argument("--skip-health", action="store_true", help="Отключить health_check")
    parser.add_argument("--sleep", type=float, default=0.7, help="Пауза между батчами, сек")
    parser.add_argument("--retries", type=int, default=5, help="Повторы KeywordTool")
    parser.add_argument("--backoff", type=float, default=1.5, help="Экспоненциальный бэкофф")
    parser.add_argument("--timeout", type=int, default=60, help="HTTP timeout, сек")
    args = parser.parse_args()

    # список стран к запуску
    countries = get_supported_countries() if args.country == "all" else [args.country]

    if not args.skip_health:
        health_check(args.api_key, countries)

    all_rows: List[Dict] = []
    for code in countries:
        rows = fetch_for_country(
            api_key=args.api_key,
            country_code=code,
            only_canon=args.no_variants,
            batch_size=args.batch_size,
            metrics_network=args.metrics_network,
            sleep_between=args.sleep,
            retries=args.retries,
            backoff=args.backoff,
            timeout=args.timeout,
        )
        all_rows.extend(rows)

    # если запускаем по нескольким странам — сохраняем общий CSV
    if len(countries) > 1:
        write_csv("all_results.csv", all_rows)
        print("Saved: all_results.csv")

    print("\n✅ Done.")


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
from typing import List, Tuple, Optional
import pandas as pd
import unicodedata
import re

from brands_catalog import (
    get_supported_countries,
    get_country_title,
    canonicalize,
)

# -------------------- helpers --------------------

def _norm_basic(s: str) -> str:
    """Простая нормализация: lower, снятие диакритики, оставляем только a-z0-9."""
    s = (str(s) if s is not None else "").strip().lower()
    s = "".join(c for c in unicodedata.normalize("NFKD", s) if not unicodedata.combining(c))
    return re.sub(r"[^a-z0-9]+", "", s)

def _results_path(cc: str) -> str:
    """Путь к CSV объёмов по стране (совместим с br_results.csv / pl_results.csv)."""
    return f"{cc.lower()}_results.csv"

def _canon(country_code: Optional[str], s: str) -> str:
    """Берём канон из каталога, иначе нормализацию."""
    if country_code:
        try:
            c = canonicalize(country_code, s)
            if c:
                return c
        except KeyError:
            # если страна вне каталога, падаем в нормализацию
            pass
    return _norm_basic(s)

# -------------------- volumes --------------------

def load_volumes(path_country: List[Tuple[str, str]]) -> pd.DataFrame:
    """
    Загружает несколько файлов объёмов, строит 'canon' и берёт максимум объёма по канону.
    Возвращает df ['canon','search_volume'].
    """
    dfs = []
    for p, cc in path_country:
        try:
            df = pd.read_csv(p)
        except FileNotFoundError:
            continue

        if "keyword" not in df.columns:
            continue

        # coalesce volume столбца
        if "volume" in df.columns and "search_volume" not in df.columns:
            df["search_volume"] = pd.to_numeric(df["volume"], errors="coerce")
        else:
            df["search_volume"] = pd.to_numeric(df.get("search_volume"), errors="coerce")
        df["search_volume"] = df["search_volume"].fillna(0)

        # ключ → canon
        df["keyword"] = df["keyword"].astype(str).str.strip()
        df["canon"] = df["keyword"].apply(lambda x: _canon(cc, x))
        dfs.append(df[["canon", "search_volume"]])

    if not dfs:
        return pd.DataFrame(columns=["canon", "search_volume"])

    vol = pd.concat(dfs, ignore_index=True)
    vol = vol.groupby("canon", as_index=False)["search_volume"].max()
    return vol

# -------------------- main --------------------

def main():
    ap = argparse.ArgumentParser(
        description="Merge volumes with Keyapp audit (canonical join) + caps/filters/sorting"
    )
    ap.add_argument("--audit", default="niche_competitors_keyapp.csv",
                    help="CSV из niche_brand_audit.py")
    ap.add_argument("--country", choices=get_supported_countries() + ["all"], default="all")
    # капы
    ap.add_argument("--cap", type=int, default=None,
                    help="Upper cap (если указан, экранирует cap-upper)")
    ap.add_argument("--cap-upper", type=int, default=None,
                    help="Upper cap (если cap не указан)")
    ap.add_argument("--cap-lower", type=int, default=None,
                    help="Lower cap (минимальный объём)")
    # фильтры
    ap.add_argument("--only-with-competitor", action="store_true",
                    help="Оставлять только строки с конкурентом (конкурент != '-')")
    ap.add_argument("--only-nonused", action="store_true",
                    help="Оставлять только строки, где Юзаный == 'Нет'")
    # сортировки
    ap.add_argument("--sort", choices=["desc", "asc"], default="desc")
    ap.add_argument("--sort-by", choices=["volume", "country"], default="volume")
    ap.add_argument("--top-per-country", type=int, default=None,
                    help="Оставить топ-N на страну после сортировки")
    ap.add_argument("--out", default="niche_competitors_keyapp_sorted.csv")
    args = ap.parse_args()

    # какие страны используем
    countries = get_supported_countries() if args.country == "all" else [args.country]

    # загрузим объёмы для выбранных стран
    vol_paths = [(_results_path(cc), cc) for cc in countries]
    # если какие-то файлы отсутствуют — просто не добавятся
    single_country = len(countries) == 1
    country_for_canon = countries[0] if single_country else None

    # аудит
    audit = pd.read_csv(args.audit)
    if audit.empty:
        pd.DataFrame(columns=["ключ", "конкурент", "Юзаный", "страна", "search_volume"]).to_csv(
            args.out, index=False, encoding="utf-8-sig"
        )
        print(f"Saved: {args.out} (empty audit)")
        return

    # ограничим страны из аудита теми, что выбраны
    include_titles = {get_country_title(cc) for cc in countries}
    audit = audit[audit["страна"].isin(include_titles)].copy()
    if audit.empty:
        pd.DataFrame(columns=["ключ", "конкурент", "Юзаный", "страна", "search_volume"]).to_csv(
            args.out, index=False, encoding="utf-8-sig"
        )
        print(f"Saved: {args.out} (no rows for selected country)")
        return

    # посчитаем canon для аудита
    if country_for_canon is not None:
        # одна страна — канон по её правилам
        audit["canon"] = audit["ключ"].apply(lambda x: _canon(country_for_canon, x))
    else:
        # несколько стран — канон отдельно по каждой строке в зависимости от страны
        title2cc = {get_country_title(cc): cc for cc in get_supported_countries()}
        audit["canon"] = audit.apply(
            lambda r: _canon(title2cc.get(r["страна"]), r["ключ"]), axis=1
        )

    # загрузка объёмов и мердж
    vol = load_volumes(vol_paths)

    merged = audit.merge(vol, on="canon", how="left")
    merged["search_volume"] = pd.to_numeric(merged.get("search_volume"), errors="coerce").fillna(0)

    # базовые фильтры
    if args.only_nonused:
        merged = merged[merged["Юзаный"].astype(str).str.strip().str.lower().eq("нет")].copy()

    has_comp = merged["конкурент"].fillna("-") != "-"

    cap_upper = args.cap if args.cap is not None else args.cap_upper
    if cap_upper is not None:
        merged = merged[has_comp | (merged["search_volume"] <= cap_upper)].copy()
    if args.cap_lower is not None:
        merged = merged[has_comp | (merged["search_volume"] >= args.cap_lower)].copy()
    if args.only_with_competitor:
        merged = merged[has_comp].copy()

    # сортировка
    if args.sort_by == "volume":
        merged = merged.sort_values("search_volume", ascending=(args.sort == "asc"))
    else:
        merged = merged.sort_values(
            ["страна", "search_volume"], ascending=[True, (args.sort == "asc")]
        )

    # топ-N на страну (опционально)
    if args.top_per_country:
        merged = (
            merged.groupby("страна", as_index=False, group_keys=False)
            .apply(lambda d: d.head(args.top_per_country))
        )

    out_cols = ["ключ", "конкурент", "Юзаный", "страна", "search_volume"]
    merged[out_cols].to_csv(args.out, index=False, encoding="utf-8-sig")
    print(f"Saved: {args.out}")

if __name__ == "__main__":
    main()

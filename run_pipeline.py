#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse, json, os, subprocess, sys
from pathlib import Path

from brands_catalog import get_supported_countries

DEFAULT_CONFIG_PATH = Path.home() / ".config" / "keyword_pipeline" / "keys.json"

def run(cmd: list[str], cwd: Path) -> None:
    print("\n$"," ".join(cmd))
    proc = subprocess.run(cmd, cwd=cwd)
    if proc.returncode != 0:
        sys.exit(proc.returncode)

def str2caps(s: str):
    if not s:
        return []
    out = []
    for x in s.split(","):
        x = x.strip()
        if not x: continue
        try:
            out.append(int(x))
        except ValueError:
            pass
    return out

def outname(country: str, cap: int | None, base: str | None) -> str:
    if base:
        return base
    cc = country.lower()
    return f"{cc}_competitors_all.csv" if cap is None else f"{cc}_competitors_cap{cap}.csv"

def _load_keys(config_path: Path | None):
    kt = os.getenv("KEYWORDTOOL_KEY")
    ka = os.getenv("KEYAPP_KEY")
    aspy = os.getenv("APPSTORESPY_KEY")
    if config_path and config_path.exists():
        try:
            data = json.loads(config_path.read_text(encoding="utf-8"))
            kt = kt or data.get("keywordtool_key")
            ka = ka or data.get("keyapp_key")
            aspy = aspy or data.get("appstorespy_key")
        except Exception:
            pass
    return kt, ka, aspy

def _save_keys(config_path: Path, kt: str, ka: str, aspy: str | None):
    config_path.parent.mkdir(parents=True, exist_ok=True)
    payload = {"keywordtool_key": kt, "keyapp_key": ka}
    if aspy:
        payload["appstorespy_key"] = aspy
    config_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"🔐 Keys saved to {config_path}")

def _mask(s: str | None, keep: int = 4) -> str:
    if not s:
        return "None"
    return s[:keep] + "…" if len(s) > keep else "****"

def main():
    p = argparse.ArgumentParser(
        description="Batch pipeline: keywordtool_fetch -> niche_brand_audit -> rank_competitors (caps) + key storage"
    )
    # ключи
    p.add_argument("--keywordtool-key", help="KeywordTool API key (или ENV/конфиг)")
    p.add_argument("--keyapp-key",      help="Keyapp API token (Bearer) (или ENV/конфиг)")
    p.add_argument("--appstorespy-key", help="AppstoreSpy API key (Bearer) (или ENV/конфиг)")
    p.add_argument("--config", default=str(DEFAULT_CONFIG_PATH), help="Путь к JSON с ключами")
    p.add_argument("--save-keys", action="store_true", help="Сохранить переданные ключи и выйти")

    # страна/кап-пакет
    supported = get_supported_countries()
    default_country = "br" if "br" in supported else (supported[0] if supported else "all")
    p.add_argument("--country", choices=supported + ["all"], default=default_country)
    p.add_argument("--caps", default="", help="Список капов: 500000,250000,100000,50000,10000,1000")
    p.add_argument("--rank-out", default=None)

    # keywordtool_fetch
    p.add_argument("--batch-size", type=int, default=700)
    p.add_argument("--metrics-network", choices=["googlesearch","googlesearchnetwork"], default="googlesearch")
    p.add_argument("--no-variants", action="store_true")
    p.add_argument("--skip-health", action="store_true")
    p.add_argument("--fetch-sleep", type=float, default=0.7)
    p.add_argument("--fetch-retries", type=int, default=5)
    p.add_argument("--fetch-backoff", type=float, default=1.5)
    p.add_argument("--fetch-timeout", type=int, default=60)

    # audit (ALL бренды; topN Play; AppstoreSpy)
    p.add_argument("--keyapp-base-url", default="https://keyapp.top/api/v2")
    p.add_argument("--audit-topn", type=int, default=10)
    p.add_argument("--audit-play-sleep", type=float, default=0.2)
    p.add_argument("--audit-aspy-sleep", type=float, default=0.15)

    # rank
    p.add_argument("--cap-lower", type=int, default=None)
    p.add_argument("--only-with-competitor", action="store_true")
    p.add_argument("--sort", choices=["desc","asc"], default="desc")
    p.add_argument("--sort-by", choices=["volume","country"], default="volume")
    p.add_argument("--top-per-country", type=int, default=None)

    # скрипты
    p.add_argument("--fetch-script", default="keywordtool_fetch.py")
    p.add_argument("--audit-script", default="niche_brand_audit.py")
    p.add_argument("--rank-script",  default="rank_competitors.py")

    p.add_argument("--only-nonused", action="store_true",
                   help="(устарело) Явно оставить только неюзанные бренды")
    p.add_argument("--include-used", action="store_true",
                   help="Не фильтровать бренды, которые уже есть в Keyapp (по умолчанию скрываем их)")

    args = p.parse_args()
    cwd = Path.cwd()
    cfg = Path(args.config) if args.config else None

    # ключи
    env_kt, env_ka, env_aspy = _load_keys(cfg)
    kt = args.keywordtool_key or env_kt
    ka = args.keyapp_key or env_ka
    aspy = args.appstorespy_key or env_aspy

    if args.save_keys:
        if not args.keywordtool_key or not args.keyapp_key:
            print("❌ Для --save-keys нужны: --keywordtool-key и --keyapp-key (AppstoreSpy опционален)")
            sys.exit(2)
        _save_keys(cfg, args.keywordtool_key, args.keyapp_key, args.appstorespy_key)
        return

    if not kt or not ka:
        print("❌ Нет ключей. Укажи флагами или задай ENV/конфиг.\n"
              "   export KEYWORDTOOL_KEY=... ; export KEYAPP_KEY=... ; export APPSTORESPY_KEY=...\n"
              "   или один раз сохрани: --save-keys --keywordtool-key ... --keyapp-key ... [--appstorespy-key ...]")
        sys.exit(2)

    print(f"🔑 Keys: KEYWORDTOOL_KEY={_mask(kt)}  KEYAPP_KEY={_mask(ka)}  APPSTORESPY_KEY={_mask(aspy)}")

    # 0) fetch (раз в страну)
    fetch_cmd = [
        sys.executable, args.fetch_script,
        "--api-key", kt,
        "--country", args.country,
        "--batch-size", str(args.batch_size),
        "--metrics-network", args.metrics_network,
        "--sleep", str(args.fetch_sleep),
        "--retries", str(args.fetch_retries),
        "--backoff", str(args.fetch_backoff),
        "--timeout", str(args.fetch_timeout),
    ]
    if args.no_variants: fetch_cmd.append("--no-variants")
    if args.skip_health: fetch_cmd.append("--skip-health")
    run(fetch_cmd, cwd)

    # 1) audit (ALL бренды; конкурент = max installs/day; banned-флаг)
    audit_out = "niche_competitors_keyapp.csv"
    audit_cmd = [
        sys.executable, args.audit_script,
        "--api-key", ka,
        "--base-url", args.keyapp_base_url,
        "--country", args.country,
        "--topn", str(args.audit_topn),
        "--play-sleep", str(args.audit_play_sleep),
        "--aspy-sleep", str(args.audit_aspy_sleep),
        "--out", audit_out,
    ]
    if aspy:
        audit_cmd += ["--appstorespy-key", aspy]
    run(audit_cmd, cwd)

    # 2) rank — пакет капов
    caps = str2caps(args.caps)
    filter_nonused = True
    if getattr(args, "include_used", False):
        filter_nonused = False
    if getattr(args, "only_nonused", False):
        filter_nonused = True
    if not caps:
        out_file = outname(args.country, None, args.rank_out)
        rank_cmd = [
            sys.executable, args.rank_script,
            "--audit", audit_out,
            "--country", args.country,
            "--sort", args.sort,
            "--sort-by", args.sort_by,
            "--out", out_file,
        ]
        if args.cap_lower is not None:
            rank_cmd += ["--cap-lower", str(args.cap_lower)]
        if args.only_with_competitor:
            rank_cmd.append("--only-with-competitor")
        if filter_nonused:
            rank_cmd.append("--only-nonused")
        if args.top_per_country is not None:
            rank_cmd += ["--top-per-country", str(args.top_per_country)]
        run(rank_cmd, cwd)
        print(f"\n✅ Pipeline done. Output: {out_file}")
        return

    for cap in caps:
        out_file = outname(args.country, cap, args.rank_out)
        rank_cmd = [
            sys.executable, args.rank_script,
            "--audit", audit_out,
            "--country", args.country,
            "--sort", args.sort,
            "--sort-by", args.sort_by,
            "--out", out_file,
            "--cap", str(cap),
        ]
        if args.cap_lower is not None:
            rank_cmd += ["--cap-lower", str(args.cap_lower)]
        if args.only_with_competitor:
            rank_cmd.append("--only-with-competitor")
        if filter_nonused:
            rank_cmd.append("--only-nonused")
        if args.top_per_country is not None:
            rank_cmd += ["--top-per-country", str(args.top_per_country)]
        run(rank_cmd, cwd)
        print(f"   ↳ wrote: {out_file}")

    print("\n✅ Batch pipeline done.")

if __name__ == "__main__":
    main()

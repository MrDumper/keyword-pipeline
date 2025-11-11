# brands_catalog.py
# -*- coding: utf-8 -*-
"""
Единый справочник стран и брендов:
- централизует Google Ads Location ID и язык для KeywordTool
- хранит канонические списки брендов (то, что попадёт в итоговые CSV)
- генерирует variants (синонимы/склейки) только для запроса и маппинга
- предоставляет функции:
    get_supported_countries() -> List[str]
    get_country_config(code)  -> {"location_id": int, "language": str}
    canonical_list(code)      -> List[str]
    variants_map(code)        -> Dict[canon, List[variant]]
    all_variants_for_country(code) -> List[str]
    canonicalize(code, s)     -> Optional[canon]
При добавлении новой страны править только этот файл.
"""

from __future__ import annotations
from typing import Dict, List, Set, Optional
import re
import unicodedata

# --------------------- НОРМАЛИЗАЦИЯ ---------------------

def normalize_text(s: str) -> str:
    """lower + remove diacritics + keep only a-z0-9"""
    s = (s or "").strip().lower()
    s = ''.join(c for c in unicodedata.normalize('NFKD', s) if not unicodedata.combining(c))
    return re.sub(r'[^a-z0-9]+', '', s)

def uniq(seq: List[str]) -> List[str]:
    out, seen = [], set()
    for x in seq:
        k = (x or "").strip()
        if not k:
            continue
        lk = k.lower()
        if lk in seen:
            continue
        seen.add(lk)
        out.append(k)
    return out

def _base_variants(canon: str) -> List[str]:
    """
    Базовые варианты: канон, lower, "склейка" без пробелов/точек/плюсов.
    Эти формы достаточны для KeywordTool/маппинга; шум не раздуваем.
    """
    v = [canon, canon.lower()]
    glued = normalize_text(canon)
    if glued and glued != canon.lower():
        v.append(glued)
    return uniq(v)

# --------------------- КОНФИГ СТРАН ---------------------

# Здесь задаются поддерживаемые страны: язык KeywordTool и Google Ads Location ID.
COUNTRIES: Dict[str, Dict[str, object]] = {
    "ar": {"language": "es", "location_id": 2032, "title": "Аргентина"},
    "br": {"language": "pt", "location_id": 2076, "title": "Бразилия"},
    "pl": {"language": "pl", "location_id": 2616, "title": "Польша"},
    # добавляй новые страны здесь (код ISO2 в нижнем регистре):
    # "mx": {"language": "es", "location_id": 2484, "title": "Мексика"},
    # "tr": {"language": "tr", "location_id": 2326, "title": "Турция"},
}

def get_supported_countries() -> List[str]:
    return sorted(COUNTRIES.keys())

def get_country_config(code: str) -> Dict[str, object]:
    code = (code or "").lower()
    if code not in COUNTRIES:
        raise KeyError(f"Unknown country code: {code}. Supported: {', '.join(get_supported_countries())}")
    return COUNTRIES[code]

# --------------------- КАНОНЫ ПО СТРАНАМ ---------------------

# ВАЖНО: канонические списки — только они попадают в итоговые CSV.
# Варианты/синонимы мы генерируем ниже и используем лишь для запросов/маппинга.

CANON_BY_COUNTRY: Dict[str, List[str]] = {
    "ar": uniq([
        "Betano","Bet365","Codere","Betsson","bplay","BetWarrior","Jugadón","City Center Online",
        "Casino Magic Online","Casino Club Online","Casino Buenos Aires Online","Palermo Online","Casino del Río Online",
        "Casino Santa Fe Online","Casino de Mendoza Online","Casino de Córdoba Online","Casino de Victoria Online",
        "Casino de Entre Ríos Online","Casino de Misiones Online","Casino de Tucumán Online","Casino de Neuquén Online",
        "Casino de Río Negro Online","Casino de San Luis Online","Casino de San Juan Online","Casino de Salta Online",
        "Casino de Chaco Online","Casino de Corrientes Online","Casino de La Pampa Online","Casino de Formosa Online",
        "Betcris","Rivalo","Betway","Betfair","Pinnacle","Marathonbet","1xBet","1win","22Bet","20Bet",
        "TonyBet","LeoVegas","Unibet","William Hill","Betfred","Bwin","888sport","888casino","Bodog","Stake",
        "BC.GAME","N1 Bet","Mostbet","Melbet","Parimatch","10bet","BetVictor","Campeonbet","Librabet","Rabona",
        "Powbet","FezBet","BetTilt","Megapari","Betobet","GG.BET","Pin-Up","PlayUZU","Vulkan Vegas","SlotV",
        "Bizzo","Neon54","7Signs","HellSpin","Tsars","1xSlots","Wazamba","BoaBoa","ZetCasino","Casumo",
        "NetBet","LV BET","Novibet","Betmotion","Spin Casino","Spinamba","Mr Green","Royal Panda","Karamba",
        "Bet-at-home","Interwetten","BetAmerica","TwinSpires","Tipico","ComeOn","Bethard","BetUK","Grosvenor Casino",
        "Coral","Ladbrokes","Paddy Power","SBK","PokerStars Sports","PokerStars Casino","PartyCasino","PartyPoker",
        "BetMGM","Caesars Sportsbook","DraftKings","FanDuel","Sky Bet","SugarHouse","888poker","GGPoker",
        "WPT Global","Winamax","Coolbet","Betsson Group (StarCasino)","StarCasino","JackpotCity","Royal Vegas",
        "Spin Sports","EnergyCasino","Mr.Play","Platin Casino","Playamo","Casimba","CasiGo","PlayOJO","Genesis Casino",
        "Kassu","Spinit","Kroon Casino","Betano Casino","Bet365 Casino","Betway Casino","Codere Casino",
        "Betsson Casino","bplay Casino","Casino Club","Casino Buenos Aires","Hipódromo Argentino de Palermo",
        "City Center Rosario","Casino Victoria","Casino Magic Neuquén","Casino Puerto Madero","Casino Trilenium",
        "Casino Tigre","Casino Pinamar","Casino Bariloche","Casino Cipolletti","Casino Maipú","Casino Godoy Cruz",
        "Casino Central Mar del Plata","Casino Miramar","Casino Santa Rosa","Casino Resistencia","Casino Posadas",
        "Casino Iguazú","Casino Salta","Casino Termas de Río Hondo","Casino Catamarca","Casino Ushuaia",
        "Casino Río Grande","Casino Mendoza Online","Boldt Gaming","Atlántica de Juegos","Alea (Lotería)",
        "Lotería de la Ciudad (BA CABA Online)","Lotería de la Provincia (Buenos Aires)","BPlay Santa Fe",
        "BPlay Entre Ríos","BPlay Buenos Aires","Betpoint","Betnacional (LATAM)","Retabet","Suertia","KirolBet",
        "Codeta","Betboro","Dafabet","12Bet","10CRIC","22Win","1Bet","Stake Originals","Thunderpick","Roobet",
        "Sportsbet.io","Cloudbet","FortuneJack","mBit Casino","Bitcasino.io","BetFury","Rollbit","BC.Game Casino",
        "Rubet","Blaze","Betano Argentina","Betway Argentina","Codere Argentina","Betsson Argentina",
        "bplay Argentina","BetWarrior Argentina","Jugadón Argentina","City Center Online Argentina",
    ]),
    "br": uniq([
    # Топ/мейнстрим
    "Betano","bet365","Sportingbet","PixBet","Betnacional","Superbet",
    "Betfair","Galera.bet","EstrelaBet","KTO","Brazino777","BetMGM","Bet7k","Vaidebet","BetPix365",
    "Esportes da Sorte","Casa de Apostas","Rivalo","Pinnacle","Betway","Betboo","Novibet","Bodog",
    "Betmotion","Dafabet","Bettilt","Betwinner","22Bet","Parimatch","LeoVegas","Betsafe","PokerStars",
    "Marathonbet","BetVictor","888casino","TonyBet","Betfred","betwarrior",

    # Авторизованные/нишевые/локальные
    "BR4Bet","Alfa.bet","VersusBet","BetCopa","Aposta Ganha","ApostaMax","Aposta1","AviaoBet",
    "Bateu Bet","MultiBet","RicoBet","BRXBet","PIN","StartBet","Luck.bet","Bet4","FYBet","TivoBet",
    "BetFast","Bravo","Tradicional","JonBet","Bet Gorillas","Bet Buffalos","Bet Falcons",
    "Reals","BRBet","B1 Bet","Apostou","OleyBet","OnaBet","BetPark","BetBoom","Matchbook",
    "BetEspecial","Bolsa de Aposta","FulliBet","BetBra","ArenaPlus","BingoPlus","SeguroBet",
    "7Games","King Panda","GingaBet","QGBet","VivaSorte","AFUN","Sortenabet","Betou","Betfusion",
    "Sorte Online","LottoLand","Tiger","PQ777","5G","BetEsporte","Lance de Sorte","SupremaBet",
    "MaximaBet","UltraBet","Bet Sul","Jogo Online","SeuBet","H2 Bet","4Win","4Play","Pagol",
    "Aposta10","Aposte Fácil","Aposta Certa","Apostou Legal","NossaBet","PlayBets","ReiBet","NextBet",
    "MrJackBet","F12Bet","PagBet","VaiBet","KakáBet","NacionalBet","BetMais","SambaBet","RioBet",
    "BrasilBet","CariocaBet","MineiroBet","GauchoBet","NordesteBet","AmazôniaBet","PantanalBet",
    "LigaBet","ArenaBet","TorcidaBet","CartolaBet","EsportivaBet","Apostou BR","Bet Prime","PrimeBet",
    "TopBet","Ultra Aposta","JetBet","FlashBet","TurboBet","HyperBet","TopPix","PixWin","PixLuck",
    "PixAposta","PixSport","PixPlay","PixGol","PixScore","PixChance","PixMaster","PixPrime",

    # Fantasy/Daily fantasy / related
    "Rei do Pitaco","Cartola FC","Matchday","Sorare",

    # iGaming / казино
    "BacanaPlay","PlayUzu","WJCasino","Cassino","Fogo777","IJogo","P9","9F","6R","Bet.app",
    "Bingo","Big","Caesars","Betsson","Blaze","Stake","Pin-Up","1win","1xBet","Mostbet","Melbet",
    "Betano Casino","KTO Casino","Betway Casino","LeoVegas Casino","PixBet Casino","EstrelaBet Casino"
]),
    "pl": uniq([
        # действующие букмекеры
        "Superbet","Betclic","STS","Fortuna","Betfan","LV BET","forBET","TOTALbet","eWinner",
        "ETOTO","PZBuk","Fuksiarz","Betcris","Betters","GO+bet",
        "AdmiralBet","Lebull","ComeOn","Traf",
        # доп. локальные/исторические для охвата
        "Noblebet","BetX","Totolotek",
        # казино/лотереи (гос./офлайн)
        "Total Casino","Casinos Poland","Hit Casino","LOTTO","Totalizator Sportowy",
    ]),
}

# --------------------- ДОП. АЛИАСЫ/ВАРИАНТЫ ---------------------

EXTRA_ALIASES_BY_COUNTRY: Dict[str, Dict[str, List[str]]] = {
    "br": {
        "Galera.bet": ["GaleraBet","galera.bet","galerabet"],
        "Casa de Apostas": ["CasadeApostas","casa de apostas","casadeapostas"],
        "Esportes da Sorte": ["esportes da sorte","Esporte365","Esporte 365"],
        "LV BET": ["LVBET","lvbet","LVBet"],  # на случай пересечений
        "GO+bet": ["GO BET","GoBet","gobet","GO+BET","go+bet"],
    },
    "pl": {
        "LV BET": ["LVBET","lvbet","LVBet"],
        "GO+bet": ["GO BET","GoBet","gobet","GO+BET","go+bet"],
        "Total Casino": ["TotalCasino","totalcasino"],
        "Casinos Poland": ["Casino Poland","casinospoland"],
        "Hit Casino": ["HitCasino","hitcasino"],
        "LOTTO": ["lotto"],
    },
}

# --------------------- ПОСТРОЕНИЕ ВАРИАНТОВ И ОБРАТНОГО ИНДЕКСА ---------------------

def _build_variants_map(canon_list: List[str], extras: Dict[str, List[str]]) -> Dict[str, List[str]]:
    out: Dict[str, List[str]] = {}
    for c in canon_list:
        vs = _base_variants(c) + extras.get(c, [])
        out[c] = uniq(vs)
    return out

def _make_reverse_index(variants_map: Dict[str, List[str]]) -> Dict[str, str]:
    rev: Dict[str, str] = {}
    for canon, vars_ in variants_map.items():
        for v in vars_:
            rev[normalize_text(v)] = canon
    return rev

# Кешируем рассчитанные мапы, чтобы не собирать каждый раз
_VARIANTS_CACHE: Dict[str, Dict[str, List[str]]] = {}
_REVERSE_CACHE: Dict[str, Dict[str, str]] = {}

def _ensure_country_built(code: str) -> None:
    code = (code or "").lower()
    if code not in CANON_BY_COUNTRY:
        raise KeyError(f"Unknown country code for brands: {code}")
    if code not in _VARIANTS_CACHE:
        canon = CANON_BY_COUNTRY[code]
        extras = EXTRA_ALIASES_BY_COUNTRY.get(code, {})
        variants = _build_variants_map(canon, extras)
        _VARIANTS_CACHE[code] = variants
        _REVERSE_CACHE[code] = _make_reverse_index(variants)

# --------------------- ПУБЛИЧНЫЕ ХЕЛПЕРЫ (используются в других скриптах) ---------------------

def canonical_list(code: str) -> List[str]:
    """Канонический список брендов (то, что пойдёт в итоговый CSV)."""
    code = (code or "").lower()
    if code not in CANON_BY_COUNTRY:
        raise KeyError(f"Unknown country code: {code}")
    return CANON_BY_COUNTRY[code][:]

def variants_map(code: str) -> Dict[str, List[str]]:
    """Словарь: canon -> [variants...] (для запросов/маппинга)."""
    _ensure_country_built(code)
    return _VARIANTS_CACHE[code]

def all_variants_for_country(code: str) -> List[str]:
    """Плоский список всех вариантов (уникальных) для страны (для запросов в API)."""
    vm = variants_map(code)
    acc: Set[str] = set()
    for arr in vm.values():
        acc.update(arr)
    # ВАЖНО: возвращаем в стабильном порядке (по алфавиту), чтобы батчи были воспроизводимы
    return sorted(acc)

def canonicalize(code: str, s: str) -> Optional[str]:
    """Вернуть канон по произвольной строке (варианту)."""
    _ensure_country_built(code)
    key = normalize_text(s)
    return _REVERSE_CACHE[code].get(key)

def get_country_title(code: str) -> str:
    """Человеко-читаемое имя страны (для таблиц/вывода)."""
    cfg = get_country_config(code)
    return str(cfg.get("title") or code.upper())

def get_country_language(code: str) -> str:
    return str(get_country_config(code)["language"])

def get_country_location_id(code: str) -> int:
    return int(get_country_config(code)["location_id"])

# --------------------- ШАБЛОН ДОБАВЛЕНИЯ НОВОЙ СТРАНЫ ---------------------
"""
Чтобы добавить новую страну (например, MX):
1) Добавь её в COUNTRIES:
   COUNTRIES["mx"] = {"language": "es", "location_id": 2484, "title": "Мексика"}

2) Добавь канон-список брендов:
   CANON_BY_COUNTRY["mx"] = ["Brand1","Brand2",...]

3) (Опционально) Добавь extras-алиасы:
   EXTRA_ALIASES_BY_COUNTRY["mx"] = {
       "Brand1": ["BrandOne","brandone"],
       ...
   }

4) Ничего в остальных файлах менять не нужно.
"""

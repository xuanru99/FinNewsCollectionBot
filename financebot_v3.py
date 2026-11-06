#!/usr/bin/env python3
"""
FinNewsCollectionBot v3.1 - 期货财经新闻智能聚合系统
========================================================
核心改进 (v3.1):
1. ✅ 新增"国内外金融要闻速览"板块: 每次推送提炼国内3-5条 + 海外4-6条当日重要金融新闻
2. ✅ 每条新闻标注精确发布时间 (北京时间, 精确到分钟)
3. ✅ 新增国内数据源 (华尔街见闻 / Google News 中文财经) 和"股市"品种类别
4. ✅ 保留 v3.0 全部能力: 时间守卫/双层去重/全品种覆盖/AI精要/微信推送

v3.0 历史:
- 北京时间 07:30 / 19:30 精准推送
- 双层去重: 批内去重 + 跨批次去重 (3天内不重复)
- 全品种覆盖: 能源/贵金属/有色/黑色/谷物/油脂/软商品/航运/化工/农副
- 专业数据源: Mining.com / OilPrice / WorldGrain / HellenicShipping 等
- 智谱 GLM-4 精要摘要 (200-300字，数据类提炼要点)
- Server酱 微信推送 + 品种分布统计
"""

import os
import sys
import json
import re
import time
import hashlib
import logging
from datetime import datetime, timezone, timedelta
from typing import Optional
from concurrent.futures import ThreadPoolExecutor, as_completed

import feedparser
import requests
from bs4 import BeautifulSoup

# ============================================================
# 配置区
# ============================================================

BEIJING_TZ = timezone(timedelta(hours=8))

# 智谱 AI (OpenAI 兼容)
OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY", "")
OPENAI_BASE_URL = os.environ.get("OPENAI_BASE_URL", "https://open.bigmodel.cn/api/paas/v4")
OPENAI_MODEL = os.environ.get("OPENAI_MODEL", "glm-4-flash")

# Server酱 —— 支持多个接收人，用逗号/分号/换行/空格任意分隔
SERVER_CHAN_KEYS = [
    k.strip() for k in re.split(r'[,;\s]+', os.environ.get("SERVER_CHAN_KEYS", ""))
    if k.strip()
]

# GitHub (用于提交状态文件)
GITHUB_TOKEN = os.environ.get("GITHUB_TOKEN", "")
GITHUB_REPO = os.environ.get("GITHUB_REPOSITORY", "")

# 时间窗口 (北京时间) —— 早晚各一次
MORNING_WINDOW = (7, 20, 11, 0)   # 07:20 - 11:00
EVENING_WINDOW = (19, 20, 23, 0)  # 19:20 - 23:00

# 状态文件
STATE_PATH = ".state/pushed.json"
DEDUP_DAYS = 3          # 跨批次去重保留天数
MAX_ARTICLES = 18       # 单次推送最多条数
MAX_PROCESS = 40        # 最多处理文章数
AI_CONCURRENCY = 4      # AI 并发数

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

# ============================================================
# 数据源 (全部实测可用，按品种细分)
# ============================================================
RSS_SOURCES = {
    # ===== 顶级权威媒体 (重点来源) =====
    "WSJ Markets": {   # 华尔街日报·市场 (RSS标题+摘要；全文付费墙走摘要提炼)
        "url": "https://feeds.a.dj.com/rss/RSSMarketsMain.xml",
        "priority": 10, "category": "金融"
    },
    "WSJ World": {     # 华尔街日报·国际/地缘
        "url": "https://feeds.a.dj.com/rss/RSSWorldNews.xml",
        "priority": 9, "category": "地缘"
    },
    "Bloomberg Markets": {
        "url": "https://feeds.bloomberg.com/markets/news.rss",
        "priority": 10, "category": "金融"
    },
    "MarketWatch Pulse": {  # 道琼斯旗下·实时快讯
        "url": "https://feeds.marketwatch.com/marketwatch/marketpulse/",
        "priority": 9, "category": "金融"
    },
    "The Hill": {      # 美国政治/政策→市场传导
        "url": "https://thehill.com/feed/",
        "priority": 6, "category": "地缘"
    },
    # Yahoo 专题 feed = 路透/彭博/美联社文章的免费全文转载
    "Yahoo 市场头条": {
        "url": "https://feeds.finance.yahoo.com/rss/2.0/headline?s=%5EGSPC&region=US&lang=en-US",
        "priority": 9, "category": "金融"
    },
    "Yahoo 黄金": {
        "url": "https://feeds.finance.yahoo.com/rss/2.0/headline?s=GC%3DF&region=US&lang=en-US",
        "priority": 9, "category": "贵金属"
    },
    "Yahoo 原油": {
        "url": "https://feeds.finance.yahoo.com/rss/2.0/headline?s=CL%3DF&region=US&lang=en-US",
        "priority": 9, "category": "能源"
    },

    # ===== 国内财经 =====
    "华尔街见闻": {
        "url": "https://dedicated.wallstreetcn.com/rss.xml",
        "priority": 9, "category": "宏观"
    },
    "Google中文财经": {
        "url": "https://news.google.com/rss/search?q=when:24h+"
               "(A%E8%82%A1+OR+%E6%B2%AA%E6%8C%87+OR+%E5%A4%AE%E8%A1%8C+OR+%E8%AF%81%E7%9B%91%E4%BC%9A"
               "+OR+%E6%9C%9F%E8%B4%A7+OR+%E9%93%81%E7%9F%BF%E7%9F%B3+OR+%E8%B4%A7%E5%B8%81%E6%94%BF%E7%AD%96"
               "+OR+%E6%9D%8E%E6%B0%8F%E6%8C%87%E6%95%B0+OR+%E7%9E%AA%E6%B2%B3%E8%82%A1%E5%B8%82)&hl=zh-CN&gl=CN&ceid=CN:zh-Hans",
        "priority": 8, "category": "宏观"
    },

    # ===== 能源 / 化工 =====
    "OilPrice.com": {
        "url": "https://oilprice.com/rss/main",
        "priority": 10, "category": "能源"
    },
    "CNBC Energy": {
        "url": "https://www.nasdaq.com/feed/rssoutbound?category=Energy",
        "priority": 9, "category": "能源"
    },

    # ===== 金属 / 矿业 (贵金属+有色+黑色) =====
    "Mining.com": {
        "url": "https://www.mining.com/feed/",
        "priority": 10, "category": "金属矿业"
    },
    "Nasdaq Commodities": {
        "url": "https://www.nasdaq.com/feed/rssoutbound?category=Commodities",
        "priority": 9, "category": "商品综合"
    },
    "Nasdaq Politics": {
        "url": "https://www.nasdaq.com/feed/rssoutbound?category=Politics",
        "priority": 6, "category": "地缘"
    },
    "FXStreet": {
        "url": "https://www.fxstreet.com/rss/news",
        "priority": 6, "category": "宏观"
    },

    # ===== 农产品 / 谷物 / 油脂 =====
    "World Grain": {
        "url": "https://www.world-grain.com/rss/articles",
        "priority": 9, "category": "谷物"
    },
    "Farm Progress": {
        "url": "https://www.farmprogress.com/rss.xml",
        "priority": 8, "category": "农产品"
    },

    # ===== 航运 =====
    "Hellenic Shipping News": {
        "url": "https://www.hellenicshippingnews.com/feed/",
        "priority": 9, "category": "航运"
    },
    "gCaptain": {
        "url": "https://gcaptain.com/feed/",
        "priority": 7, "category": "航运"
    },
    "Splash247": {
        "url": "https://splash247.com/feed/",
        "priority": 7, "category": "航运"
    },

    # ===== 综合财经 / 地缘 =====
    "CNBC Markets": {
        "url": "https://search.cnbc.com/rs/search/combinedcms/view.xml?partnerId=wrss01&id=100003114",
        "priority": 8, "category": "金融"
    },
    "CNBC Economy": {
        "url": "https://search.cnbc.com/rs/search/combinedcms/view.xml?partnerId=wrss01&id=20910258",
        "priority": 8, "category": "宏观"
    },
    "CNBC World": {
        "url": "https://search.cnbc.com/rs/search/combinedcms/view.xml?partnerId=wrss01&id=100727362",
        "priority": 7, "category": "地缘"
    },
    "MarketWatch Top": {
        "url": "https://feeds.marketwatch.com/marketwatch/topstories/",
        "priority": 8, "category": "金融"
    },
    "Guardian Business": {
        "url": "https://www.theguardian.com/business/rss",
        "priority": 6, "category": "金融"
    },
    "Guardian World": {
        "url": "https://www.theguardian.com/world/rss",
        "priority": 5, "category": "地缘"
    },
    "Bloomberg Markets": {
        "url": "https://feeds.bloomberg.com/markets/news.rss",
        "priority": 10, "category": "金融"
    },
    "Reuters via Google": {
        "url": "https://news.google.com/rss/search?q=site:reuters.com+(commodities+OR+oil+OR+metals+OR+grain)&hl=en-US&gl=US&ceid=US:en",
        "priority": 9, "category": "商品综合"
    },
}

# ============================================================
# 品种关键词体系 (全品种覆盖)
# ============================================================
PRODUCT_KEYWORDS = {
    "能源": [
        'crude oil', 'brent', 'wti', 'west texas intermediate', 'oil price', 'oil prices',
        'petroleum', 'gasoline', 'diesel', 'heating oil', 'fuel oil', 'refinery', 'refining',
        'natural gas', 'lng', 'liquefied natural gas', 'gasoline crack',
        'opec', 'opec+', 'saudi', 'aramco', 'shale', 'drilling rig', 'eia inventory',
        'coal', 'thermal coal', 'coking coal', 'coal mine', 'coal price',
        'naphtha', 'propane', 'butane', 'lpg', 'bitumen', 'asphalt',
    ],
    "贵金属": [
        'gold', 'silver', 'platinum', 'palladium', 'rhodium', 'precious metal', 'precious metals',
        'bullion', 'gold price', 'gold prices', 'xau', 'xag', 'comex gold',
        'gold etf', 'central bank gold', 'safe haven',
    ],
    "有色金属": [
        'copper', 'aluminium', 'aluminum', 'zinc', 'nickel', 'lead', 'tin', 'alumina',
        'base metal', 'base metals', 'lme', 'shanghai metal',
        'copper mine', 'smelter', 'refined copper', 'copper concentrate',
        'lithium', 'cobalt', 'rare earth', 'graphite', 'molybdenum', 'antimony',
    ],
    "黑色金属": [
        'iron ore', 'steel', 'rebar', 'hot rolled coil', 'hrc', 'wire rod', 'steel billet',
        'blast furnace', 'coke', 'coking coal', 'ferrosilicon', 'silicomanganese',
        'stainless steel', 'scrap steel', 'steel mill', 'steel output', 'steel demand',
        'direct reduced iron', 'dri', 'pig iron',
    ],
    "谷物": [
        'wheat', 'corn', 'maize', 'soybean', 'soybeans', 'rice', 'barley', 'sorghum',
        'oats', 'rye', 'grain', 'grains', 'cereal', 'harvest', 'crop',
        'usda', 'wasde', 'crop report', 'planting', 'yield estimate',
        'wheat price', 'corn price', 'soybean price',
    ],
    "油脂油料": [
        'soybean oil', 'soybean meal', 'palm oil', 'rapeseed oil', 'canola', 'canola oil',
        'sunflower oil', 'coconut oil', 'peanut', 'groundnut', 'oilseed', 'oilseeds',
        'vegetable oil', 'edible oil', 'crushing margin', 'soymeal', 'soyoil',
    ],
    "软商品": [
        'cotton', 'sugar', 'coffee', 'cocoa', 'robusta', 'arabica',
        'rubber', 'natural rubber', 'orange juice', 'lumber', 'wool',
        'sugar price', 'coffee price', 'cocoa price',
    ],
    "农副产品": [
        'hog', 'hogs', 'pork', 'live cattle', 'feeder cattle', 'cattle',
        'egg', 'eggs', 'poultry', 'chicken', 'apple', 'jujube', 'pulp',
        'dairy', 'milk', 'butter', 'cheese',
    ],
    "化工": [
        'ethylene', 'propylene', 'polyethylene', 'polypropylene', 'pvc', 'pta', 'px',
        'methanol', 'ethylene glycol', 'meg', 'styrene', 'benzene', 'toluene',
        'urea', 'ammonia', 'soda ash', 'glass', 'caustic soda', 'chlor-alkali',
        'petrochemical', 'chemical plant', 'cracker',
    ],
    "航运": [
        'baltic dry index', 'bdi', 'baltic exchange', 'freight rate', 'freight rates',
        'shipping rate', 'container rate', 'tanker rate', 'dry bulk', 'bulker',
        'vlcc', 'suez canal', 'panama canal', 'red sea', 'port congestion',
        'shipping', 'vessel', 'cargo', 'tonnage', 'charter rate',
    ],
    "地缘": [
        'geopolitical', 'geopolitics', 'sanction', 'sanctions', 'embargo', 'export ban',
        'trade war', 'tariff', 'tariffs', 'import duty', 'retaliation',
        'middle east', 'iran', 'israel', 'gaza', 'lebanon', 'hezbollah', 'houthi',
        'ukraine', 'russia', 'putin', 'zelensky', 'black sea',
        'taiwan strait', 'south china sea', 'north korea',
        'coup', 'civil war', 'military strike', 'air strike', 'conflict escalation',
    ],
    "宏观金融": [
        'federal reserve', 'fed ', 'fomc', 'powell', 'interest rate', 'rate hike', 'rate cut',
        'ecb', 'lagarde', 'boj', 'bank of japan', 'pboc', "people's bank of china",
        'central bank', 'monetary policy', 'quantitative easing', 'tapering',
        'inflation', 'cpi', 'ppi', 'deflation', 'stagflation', 'recession', 'gdp',
        'nonfarm payroll', 'jobless claims', 'unemployment', 'pmi', 'ism',
        'us dollar', 'dollar index', 'dxy', 'treasury yield', 'bond yield',
        'stock market', 's&p 500', 'nasdaq', 'dow jones', 'vix', 'equities',
        'banking crisis', 'credit risk', 'default', 'sovereign debt',
        'bitcoin', 'crypto', 'ethereum',
        # 中文宏观词
        '央行', '中国人民银行', '降准', '降息', '加息', 'LPR', '货币政策', '公开市场操作',
        '美联储', 'fomc会议', '通胀', 'cpi同比', 'ppi同比', 'pmi', 'gdp', '财政政策',
        '国务院', '证监会', '金融监管总局', '发改委', '汇率', '人民币汇率', '逆回购',
        '中期借贷便利', 'mlf', '社融', '进出口', '贸易顺差', '外汇储备',
    ],
    "股市": [
        'stock market', 's&p 500', 'nasdaq', 'dow jones', 'equities', 'shares',
        'shanghai composite', 'hang seng', 'nikkei', 'ftse 100', 'dax',
        'a-share', 'a shares', 'csi 300', 'csi300', 'ipo', 'earnings', 'valuation',
        'bull market', 'bear market', 'margin trading', 'northbound',
        # 中文股市词
        'A股', '沪指', '上证指数', '深证成指', '创业板', '科创板', '沪深300', '中证500',
        '港股', '恒生指数', '恒生科技', '北向资金', '南向资金', '涨停', '跌停',
        '股市', '股票', '纳指', '标普500', '道指', '两融', 'IPO', '印花税',
        '股价', '市值', '券商', '基金', '公募', '私募', '散户', '成交额',
    ],
}

# 权重：品种关键词 > 股市/地缘 > 宏观
CATEGORY_WEIGHTS = {
    "能源": 3.0, "贵金属": 3.0, "有色金属": 3.0, "黑色金属": 3.0,
    "谷物": 3.0, "油脂油料": 3.0, "软商品": 3.0, "农副产品": 3.0,
    "化工": 3.0, "航运": 3.0,
    "股市": 2.0, "地缘": 2.0, "宏观金融": 1.5,
}

# 要闻速览候选类别 (用于"国内外金融要闻提炼"板块)
DIGEST_CATS = {'宏观金融', '股市', '地缘', '商品综合', '宏观', '金融'}
DIGEST_SOURCES = {'华尔街见闻', 'Google中文财经'}  # 国内源优先进入国内要闻候选


# ============================================================
# 去重 & 状态管理
# ============================================================
def normalize_title(title: str) -> str:
    """标题归一化：小写、去标点、去常见前后缀"""
    t = title.lower()
    t = re.sub(r'[^a-z0-9\s]', ' ', t)
    t = re.sub(r'\s+', ' ', t).strip()
    # 去掉常见新闻前缀
    for prefix in ['exclusive ', 'update ', 'breaking ', 'analysis ']:
        if t.startswith(prefix):
            t = t[len(prefix):]
    return t


def title_hash(title: str) -> str:
    return hashlib.md5(normalize_title(title).encode()).hexdigest()[:16]


STOPWORDS = set(['the', 'a', 'an', 'of', 'to', 'in', 'on', 'for', 'and', 'or', 'as',
                 'at', 'by', 'is', 'are', 'was', 'were', 'with', 'from', 'its', 'it',
                 'this', 'that', 'be', 'has', 'have', 'will', 'after', 'over', 'new'])


def title_tokens(title: str) -> set:
    return set(w for w in normalize_title(title).split() if w not in STOPWORDS and len(w) > 2)


def is_similar(t1: str, t2: str, threshold: float = 0.55) -> bool:
    """标题相似度判断 (Jaccard + 交集词兜底)"""
    a, b = title_tokens(t1), title_tokens(t2)
    if not a or not b:
        return False
    inter = len(a & b)
    union = len(a | b)
    j = inter / union
    if j >= threshold:
        return True
    # 兜底：共享 3 个以上实义词且有一定重叠
    if inter >= 3 and j >= 0.25:
        return True
    return False


def event_similar(e1: str, e2: str) -> bool:
    """
    同一事件判断。优先解析结构化标签 "品种+方向|驱动因素"
    例: "金价上涨|美国中期选举民主党胜选"
    """
    if not e1 or not e2:
        return False

    from difflib import SequenceMatcher

    def split_ev(e):
        for sep in ('|', '｜', ':', '：'):
            if sep in e:
                a, b = e.split(sep, 1)
                return a.strip(), b.strip()
        return e.strip(), ''

    a1, d1 = split_ev(e1)
    a2, d2 = split_ev(e2)

    # 结构化比较：主判定相同 + 驱动因素相近
    if d1 and d2:
        subj = SequenceMatcher(None, a1, a2).ratio()
        drive = SequenceMatcher(None, d1, d2).ratio()
        if subj >= 0.55 and drive >= 0.35:
            return True
        # 品种+方向几乎一致，驱动有交集
        if subj >= 0.80 and drive >= 0.25:
            return True
        # 驱动因素几乎一致也判定重复
        if drive >= 0.70:
            return True
        return False

    # 非结构化兜底
    return SequenceMatcher(None, e1, e2).ratio() >= 0.35


def load_state() -> dict:
    default = {"morning_date": "", "evening_date": "", "hashes": {}, "events": []}
    try:
        with open(STATE_PATH, 'r', encoding='utf-8') as f:
            data = json.load(f)
            data.setdefault("morning_date", "")
            data.setdefault("evening_date", "")
            data.setdefault("hashes", {})
            data.setdefault("events", [])
            return data
    except Exception:
        return default


def save_state(state: dict):
    """本地保存 + 通过 GitHub API 提交（供下次运行读取）"""
    try:
        os.makedirs(os.path.dirname(STATE_PATH), exist_ok=True)
        with open(STATE_PATH, 'w', encoding='utf-8') as f:
            json.dump(state, f, ensure_ascii=False, indent=2)
        logger.info(f"状态文件已写入: {STATE_PATH}")
    except Exception as e:
        logger.warning(f"写入状态文件失败: {e}")
        return

    if not GITHUB_TOKEN or not GITHUB_REPO:
        logger.info("未配置 GITHUB_TOKEN，跳过状态文件提交")
        return

    try:
        api = f"https://api.github.com/repos/{GITHUB_REPO}/contents/{STATE_PATH}"
        headers = {'Authorization': f'token {GITHUB_TOKEN}',
                   'Accept': 'application/vnd.github.v3+json'}

        # 取当前文件 sha
        sha = None
        r = requests.get(api, headers=headers, timeout=15)
        if r.status_code == 200:
            sha = r.json().get('sha')

        import base64
        with open(STATE_PATH, 'rb') as f:
            content_b64 = base64.b64encode(f.read()).decode()

        payload = {
            'message': f'chore: update push state [{datetime.now(BEIJING_TZ).strftime("%Y-%m-%d %H:%M")}]',
            'content': content_b64,
        }
        if sha:
            payload['sha'] = sha

        r = requests.put(api, headers=headers, json=payload, timeout=20)
        if r.status_code in (200, 201):
            logger.info("状态文件已提交到仓库")
        else:
            logger.warning(f"状态文件提交失败: {r.status_code} {r.text[:150]}")
    except Exception as e:
        logger.warning(f"状态文件提交异常: {e}")


def prune_hashes(state: dict) -> dict:
    """清理超过 DEDUP_DAYS 的 hash / event 记录"""
    cutoff = datetime.now(BEIJING_TZ) - timedelta(days=DEDUP_DAYS)
    kept = {}
    for h, ts in state.get('hashes', {}).items():
        try:
            dt = datetime.fromisoformat(ts)
            if dt >= cutoff:
                kept[h] = ts
        except Exception:
            continue
    state['hashes'] = kept

    kept_events = []
    for ev in state.get('events', []):
        try:
            dt = datetime.fromisoformat(ev.get('ts', ''))
            if dt >= cutoff:
                kept_events.append(ev)
        except Exception:
            continue
    # 最多保留 300 条
    state['events'] = kept_events[-300:]
    return state


# ============================================================
# 时间守卫
# ============================================================
def check_time_window() -> tuple:
    """
    返回 (应该运行?, 时段名)
    时段: 'morning' / 'evening' / ''
    """
    now = datetime.now(BEIJING_TZ)
    h, m = now.hour, now.minute
    cur = h * 60 + m

    def in_win(win):
        s = win[0] * 60 + win[1]
        e = win[2] * 60 + win[3]
        return s <= cur <= e

    if in_win(MORNING_WINDOW):
        return True, 'morning'
    if in_win(EVENING_WINDOW):
        return True, 'evening'
    return False, ''


# ============================================================
# 文章抓取
# ============================================================
# 付费墙/反爬严格域名：跳过全文爬取，直接用 RSS 摘要提炼
PAYWALL_DOMAINS = ('wsj.com', 'ft.com', 'bloomberg.com/news/articles')


def get_article_full_text(url: str, timeout: int = 12) -> Optional[str]:
    if any(d in url for d in PAYWALL_DOMAINS):
        logger.debug(f"付费墙域名，跳过全文: {url[:60]}")
        return None
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
                      '(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.9',
    }
    try:
        resp = requests.get(url, headers=headers, timeout=timeout, allow_redirects=True)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, 'html.parser')
        for tag in soup(['script', 'style', 'nav', 'footer', 'header', 'aside', 'iframe', 'noscript']):
            tag.decompose()

        # JSON-LD
        for script in soup.find_all('script', type='application/ld+json'):
            try:
                data = json.loads(script.string or '')
                items = data if isinstance(data, list) else [data]
                for item in items:
                    if isinstance(item, dict) and item.get('articleBody'):
                        body = item['articleBody']
                        if len(body) > 300:
                            return body
            except Exception:
                continue

        # 常见容器
        for sel in ['article', '[itemprop="articleBody"]', '.article-body', '.article-content',
                    '.post-content', '.entry-content', '.story-body', '.news-content',
                    '#article-body', 'main']:
            elem = soup.select_one(sel)
            if elem:
                text = elem.get_text(separator='\n', strip=True)
                if len(text) > 250:
                    return '\n'.join(l.strip() for l in text.split('\n') if l.strip())

        # 段落兜底
        ps = [p.get_text(strip=True) for p in soup.find_all('p')]
        ps = [p for p in ps if len(p) > 50]
        if ps and len(' '.join(ps)) > 250:
            return '\n'.join(ps)

        body = soup.find('body')
        if body:
            return body.get_text(separator='\n', strip=True)[:4000]
        return None
    except Exception as e:
        logger.debug(f"全文抓取失败 [{url[:60]}]: {e}")
        return None


def fetch_rss_feeds() -> list:
    articles = []
    seen_urls = set()

    def fetch_one(name, cfg):
        out = []
        try:
            resp = requests.get(cfg['url'], headers={
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
                              '(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
            }, timeout=15)
            if resp.status_code != 200:
                return name, out, f"HTTP {resp.status_code}"
            feed = feedparser.parse(resp.content)
            for entry in feed.entries[:25]:
                link = entry.get('link', '')
                if not link:
                    continue
                published = None
                for attr in ('published_parsed', 'updated_parsed'):
                    p = getattr(entry, attr, None)
                    if p:
                        published = datetime(*p[:6], tzinfo=timezone.utc)
                        break
                if published:
                    age_h = (datetime.now(timezone.utc) - published).total_seconds() / 3600
                    if age_h > 48:
                        continue
                out.append({
                    'title': entry.get('title', '').strip(),
                    'link': link,
                    'summary': entry.get('summary', '') or entry.get('description', ''),
                    'source': name,
                    'category': cfg.get('category', '综合'),
                    'priority': cfg.get('priority', 5),
                    'published': published,
                })
            return name, out, f"{len(out)}篇"
        except Exception as e:
            return name, out, f"{type(e).__name__}"

    with ThreadPoolExecutor(max_workers=8) as ex:
        futs = {ex.submit(fetch_one, k, v): k for k, v in RSS_SOURCES.items()}
        for f in as_completed(futs):
            name, items, info = f.result()
            logger.info(f"  {name}: {info}")
            for it in items:
                if it['link'] in seen_urls:
                    continue
                seen_urls.add(it['link'])
                articles.append(it)

    logger.info(f"共获取 {len(articles)} 篇原始文章")
    return articles


# ============================================================
# 关键词匹配
# ============================================================
_KW_CACHE = {}


def _kw_pattern(kw: str):
    """编译词边界正则，避免 rice 匹配 prices / lead 匹配 leading 之类误判"""
    if kw not in _KW_CACHE:
        _KW_CACHE[kw] = re.compile(r'(?<![a-z])' + re.escape(kw.strip()) + r'(?![a-z])', re.I)
    return _KW_CACHE[kw]


def _has_kw(kw: str, text_lower: str) -> bool:
    kw = kw.strip().lower()
    if not kw:
        return False
    if kw not in text_lower:      # 快速预筛
        return False
    return _kw_pattern(kw).search(text_lower) is not None


def keyword_match(title: str, summary: str) -> dict:
    title_l = title.lower()
    full_l = (title + " " + summary).lower()

    score = 0.0
    matched_cats = []
    matched_terms = []

    for cat, kws in PRODUCT_KEYWORDS.items():
        hit = [kw for kw in kws if _has_kw(kw, full_l)]
        if hit:
            weight = CATEGORY_WEIGHTS.get(cat, 1.0)
            title_hits = sum(1 for kw in hit if _has_kw(kw, title_l))
            score += weight * min(len(hit), 5) + title_hits * weight * 1.5
            matched_cats.append(cat)
            matched_terms.extend(hit[:3])

    return {'score': score, 'categories': matched_cats, 'terms': matched_terms}


# ============================================================
# AI 处理 (精要摘要)
# ============================================================
def ai_process(article: dict, max_retries: int = 2) -> Optional[dict]:
    if not OPENAI_API_KEY:
        return None

    title = article.get('title', '')
    content = article.get('full_text', '') or article.get('summary', '')
    cats = article.get('kw', {}).get('categories', [])

    if len(content) > 5000:
        content = content[:5000] + "\n...(已截断)"

    prompt = f"""你是国际大宗商品与宏观策略分析师。阅读下面这篇新闻（可能是中文或英文），输出中文精要。

【已识别的品种标签】: {', '.join(cats) if cats else '待判断'}

要求：
1. 相关性评分 1-10（10=对商品期货/股市/宏观交易极重要）
2. 如果与以下领域**完全无关**（纯科技产品发布、娱乐八卦、体育赛事、社会轶事），只输出 SKIP
3. 输出 200-350 字中文精要，必须包含：
   - 【核心事实】发生了什么（数字、主体、时间）
   - 【市场影响】对哪些品种/资产有影响，方向（利多/利空）
   - 若原文含关键数据（价格、库存、产量、持仓、百分比），用"· "逐条列出
4. 不要逐句翻译，要提炼。面向专业投资者，语言精炼。
5. 来源权威性：WSJ、Bloomberg、Reuters、MarketWatch 等一线媒体的报道，忠实采用其表述和数据，不要演绎补充；数据以原文为准。
6. 如果"正文"部分很短（只有导语/摘要级别），基于已有信息输出 80-150 字精炼短条目，只写确定的信息，禁止编造细节和数据。

标题: {title}
来源: {article.get('source', '')}

正文:
{content}

严格按以下格式输出：
【评分】X
【事件】只输出**一个**核心事件，格式严格为"品种+方向|驱动因素"，禁止用 / 或 ; 分隔多个事件。例：
  金价上涨|美国中期选举民主党胜选
  油价反弹|中东供应中断担忧
  铜价创新高|LME库存降至低位
  同一事件在不同媒体的报道中必须使用近乎相同的表述。
【标签】标签1,标签2
【摘要】
核心事实：...
市场影响：...
关键数据：
· ...
"""

    headers = {'Authorization': f'Bearer {OPENAI_API_KEY}', 'Content-Type': 'application/json'}

    for attempt in range(max_retries + 1):
        try:
            resp = requests.post(
                f"{OPENAI_BASE_URL}/chat/completions",
                headers=headers,
                json={
                    "model": OPENAI_MODEL,
                    "messages": [
                        {"role": "system", "content": "你是专业的大宗商品研究分析师，擅长提炼关键信息。"},
                        {"role": "user", "content": prompt}
                    ],
                    "temperature": 0.3,
                    "max_tokens": 1200,
                },
                timeout=90
            )
            if resp.status_code != 200:
                raise RuntimeError(f"HTTP {resp.status_code}: {resp.text[:150]}")

            text = resp.json()['choices'][0]['message']['content'].strip()

            if text.upper().startswith('SKIP') or '【摘要】' not in text:
                if text.upper().startswith('SKIP'):
                    logger.info(f"  AI-SKIP: {title[:45]}")
                    return None

            score_m = re.search(r'【评分】\s*[:：]?\s*(\d+)', text)
            event_m = re.search(r'【事件】\s*[:：]?\s*(.+)', text)
            tag_m = re.search(r'【标签】\s*[:：]?\s*(.+)', text)
            sum_m = re.search(r'【摘要】\s*[:：]?\s*\n?(.*)', text, re.DOTALL)

            event_raw = event_m.group(1).strip() if event_m else ''
            # 兜底：AI 若返回多个事件，只保留第一个
            for sep in (' / ', '；', ';', '/'):
                if sep in event_raw:
                    event_raw = event_raw.split(sep)[0].strip()
                    break

            return {
                'score': int(score_m.group(1)) if score_m else 5,
                'event': event_raw,
                'tags': tag_m.group(1).strip() if tag_m else '',
                'summary': sum_m.group(1).strip() if sum_m else text,
            }
        except Exception as e:
            logger.warning(f"  AI失败({attempt+1}/{max_retries+1}): {str(e)[:80]}")
            if attempt < max_retries:
                time.sleep(2)

    return None


# ============================================================
# 主处理流程
# ============================================================
def process(articles: list, state: dict) -> tuple:
    """返回 (最终入选文章, 要闻速览候选池)"""
    now_bj = datetime.now(BEIJING_TZ)
    recent_hashes = state.get('hashes', {})
    recent_events = [e.get('key', '') for e in state.get('events', [])]
    recent_titles = []

    # ---- 1. 关键词初筛 + 跨批次去重 ----
    candidates = []
    for art in articles:
        title = art['title']
        h = title_hash(title)

        # 跨批次去重（同标题）
        if h in recent_hashes:
            logger.debug(f"  已推送过，跳过: {title[:50]}")
            continue

        kw = keyword_match(title, art.get('summary', ''))
        if kw['score'] < 1.5:
            continue

        # 批内标题相似去重
        if any(is_similar(title, t) for t in recent_titles):
            logger.info(f"  批内重复跳过: {title[:55]}")
            continue
        recent_titles.append(title)

        art['kw'] = kw
        art['hash'] = h
        candidates.append(art)

    logger.info(f"初筛+去重后: {len(candidates)} 篇 (原始 {len(articles)} 篇)")

    # 按关键词分 + 源优先级排序
    candidates.sort(key=lambda x: (x['kw']['score'], x['priority']), reverse=True)
    to_process = candidates[:MAX_PROCESS]
    logger.info(f"进入 AI 处理: {len(to_process)} 篇")

    # ---- 2. 并发抓全文 + AI ----
    def handle(art):
        full = get_article_full_text(art['link'])
        art['full_text'] = full if full else art.get('summary', '')
        return ai_process(art)

    results = []
    with ThreadPoolExecutor(max_workers=AI_CONCURRENCY) as ex:
        futs = {ex.submit(handle, a): a for a in to_process}
        for i, f in enumerate(as_completed(futs), 1):
            art = futs[f]
            try:
                r = f.result()
                if r:
                    art['ai'] = r
                    results.append(art)
                    logger.info(f"  [{i}/{len(to_process)}] ✓ {art['title'][:42]} ({r['score']}/10)")
            except Exception as e:
                logger.warning(f"  处理异常: {e}")

    # ---- 3. 事件级去重 + 排序 ----
    results.sort(key=lambda x: (x['ai']['score'], x['kw']['score']), reverse=True)

    deduped = []
    kept_events = []          # 本批次内的事件
    dropped = 0
    for art in results:
        ev = art['ai'].get('event', '')
        # 与本批次已选事件比较
        if ev and any(event_similar(ev, e) for e in kept_events):
            dropped += 1
            logger.info(f"  同事件跳过: {art['title'][:45]} ← {ev}")
            continue
        # 与历史事件比较（跨批次）
        if ev and any(event_similar(ev, e) for e in recent_events):
            dropped += 1
            logger.info(f"  历史同事件跳过: {art['title'][:45]} ← {ev}")
            continue
        if ev:
            kept_events.append(ev)
        deduped.append(art)

    logger.info(f"事件去重: -{dropped} 条，剩 {len(deduped)} 篇")

    # ---- 4. 品种配额选取 ----
    COMMODITY_CATS = ['能源', '贵金属', '有色金属', '黑色金属', '谷物',
                      '油脂油料', '软商品', '农副产品', '化工', '航运']
    PER_CAT_MIN = 2      # 每品种至少争取的条数
    PER_CAT_CAP = 3      # 单品种最多条数

    picked = []
    picked_set = set()
    cat_count = {}

    # 第一轮：每个商品品种取 2 条（优先高分）
    for cat in COMMODITY_CATS:
        for art in deduped:
            if len(picked) >= MAX_ARTICLES:
                break
            if art['hash'] in picked_set:
                continue
            if cat in art['kw']['categories'] and cat_count.get(cat, 0) < PER_CAT_MIN:
                picked.append(art)
                picked_set.add(art['hash'])
                for c in art['kw']['categories']:
                    cat_count[c] = cat_count.get(c, 0) + 1

    # 第二轮：按分数补齐（单品种不超过 PER_CAT_CAP）
    for art in deduped:
        if len(picked) >= MAX_ARTICLES:
            break
        if art['hash'] in picked_set:
            continue
        prim = art['kw']['categories'][0] if art['kw']['categories'] else '其他'
        if cat_count.get(prim, 0) >= PER_CAT_CAP:
            continue
        picked.append(art)
        picked_set.add(art['hash'])
        cat_count[prim] = cat_count.get(prim, 0) + 1

    picked.sort(key=lambda x: x['ai']['score'], reverse=True)
    logger.info(f"最终入选: {len(picked)} 篇")

    # ---- 5. 要闻速览候选池 (宏观/股市/地缘 + 国内源) ----
    digest_pool = [
        a for a in deduped
        if (set(a['kw']['categories']) & DIGEST_CATS) or a['source'] in DIGEST_SOURCES
    ]
    digest_pool.sort(key=lambda x: x['ai']['score'], reverse=True)
    digest_pool = digest_pool[:16]
    logger.info(f"要闻速览候选池: {len(digest_pool)} 条")

    # ---- 6. 更新状态 ----
    for art in picked:
        state['hashes'][art['hash']] = now_bj.isoformat()
        ev = art['ai'].get('event', '')
        if ev:
            state['events'].append({'key': ev, 'ts': now_bj.isoformat()})
    state = prune_hashes(state)

    return picked, digest_pool


# ============================================================
# 要闻速览 (国内外金融要闻提炼)
# ============================================================
def fmt_time(art: dict) -> str:
    """文章发布时间 → 北京时间 'MM-DD HH:MM'，缺失返回'时间未标'"""
    p = art.get('published')
    if not p:
        return '时间未标'
    return p.astimezone(BEIJING_TZ).strftime('%m-%d %H:%M')


def digest_fallback(pool: list) -> dict:
    """AI 不可用时的兜底：直接列候选标题"""
    out = {'domestic': [], 'overseas': []}
    for a in pool:
        line = f"**{fmt_time(a)}**｜{a['title']}（{a['source']}）"
        if a['source'] in DIGEST_SOURCES:
            out['domestic'].append(line)
        else:
            out['overseas'].append(line)
    out['domestic'] = out['domestic'][:5]
    out['overseas'] = out['overseas'][:6]
    return out


def ai_digest(pool: list) -> dict:
    """调用 AI 提炼国内外金融要闻速览。返回 {'domestic': [...], 'overseas': [...]}"""
    if not pool:
        return {'domestic': [], 'overseas': []}
    fallback = digest_fallback(pool)

    if not OPENAI_API_KEY:
        logger.info("未配置 AI Key，要闻速览使用兜底列表")
        return fallback

    items_text = []
    for i, a in enumerate(pool, 1):
        cats = '/'.join(a['kw']['categories'][:2])
        brief = (a['ai'].get('summary', '') or '')[:150]
        items_text.append(
            f"{i}. [发布时间: 北京时间 {fmt_time(a)}] [来源: {a['source']}] [类别: {cats}]\n"
            f"   标题: {a['title']}\n"
            f"   摘要: {brief}"
        )
    now = datetime.now(BEIJING_TZ)
    prompt = f"""你是财经新闻编辑。下面是过去一天抓取到的财经新闻池（生成时间：北京时间 {now.strftime('%Y-%m-%d %H:%M')}）。请提炼出**当日国内外重要金融要闻速览**。

要求：
1. 输出两部分：【国内要闻】3-5 条、【海外要闻】4-6 条（国内指中国市场相关，海外指国际市场）
2. 只选对股市、期货、汇率、货币政策、宏观经济、地缘局势真正重要的新闻；同一事件只出现一次
3. 每条一行，格式严格为：- HH:MM｜要点（来源）
   HH:MM 使用条目给出的北京时间发布时刻；若为"时间未标"则省略"HH:MM｜"部分
4. 要点需包含关键数字/主体/影响方向，25-45 字，按重要性排序
5. 严禁编造新闻池中没有的信息；若国内候选不足 3 条，有几条写几条

新闻池：
{chr(10).join(items_text)}

严格按以下格式输出：
【国内要闻】
- HH:MM｜要点（来源）
【海外要闻】
- HH:MM｜要点（来源）
"""

    headers = {'Authorization': f'Bearer {OPENAI_API_KEY}', 'Content-Type': 'application/json'}

    for attempt in range(2):
        try:
            resp = requests.post(
                f"{OPENAI_BASE_URL}/chat/completions",
                headers=headers,
                json={
                    "model": OPENAI_MODEL,
                    "messages": [
                        {"role": "system", "content": "你是专业的财经新闻编辑，擅长提炼当日要闻。"},
                        {"role": "user", "content": prompt}
                    ],
                    "temperature": 0.3,
                    "max_tokens": 900,
                },
                timeout=90
            )
            if resp.status_code != 200:
                raise RuntimeError(f"HTTP {resp.status_code}: {resp.text[:150]}")

            text = resp.json()['choices'][0]['message']['content'].strip()

            def extract(block: str) -> list:
                out = []
                for ln in block.splitlines():
                    ln = ln.strip()
                    if ln.startswith('-'):
                        item = ln.lstrip('- ').strip()
                        if item:
                            out.append(item)
                return out

            dom_m = re.search(r'【国内要闻】(.*?)(?=【海外要闻】|$)', text, re.DOTALL)
            ovs_m = re.search(r'【海外要闻】(.*)', text, re.DOTALL)
            d = extract(dom_m.group(1)) if dom_m else []
            o = extract(ovs_m.group(1)) if ovs_m else []
            if d or o:
                logger.info(f"要闻速览生成成功: 国内 {len(d)} 条 / 海外 {len(o)} 条")
                return {'domestic': d[:5], 'overseas': o[:6]}
            logger.warning(f"要闻速览解析为空 (尝试 {attempt + 1}/2)")
        except Exception as e:
            logger.warning(f"要闻速览生成失败({attempt + 1}/2): {str(e)[:80]}")

    logger.info("要闻速览使用兜底列表")
    return fallback


# ============================================================
# 推送格式
# ============================================================
def format_push(articles: list, period: str, digest: dict = None) -> str:
    now = datetime.now(BEIJING_TZ)
    period_cn = "早报" if period == 'morning' else "晚报"
    digest = digest or {'domestic': [], 'overseas': []}

    # 品种分布统计
    dist = {}
    for a in articles:
        for c in a['kw']['categories']:
            dist[c] = dist.get(c, 0) + 1
    # 按数量排序
    dist_line = " | ".join(f"{k} {v}" for k, v in sorted(dist.items(), key=lambda x: -x[1])[:10])

    lines = [
        f"# 🌍 财经·期货要闻 {period_cn}",
        f"> **{now.strftime('%Y年%m月%d日 %H:%M')}** | 共 **{len(articles)}** 条",
        "",
        f"**品种分布**: {dist_line}",
        "",
        "---",
        "",
    ]

    # ---- 国内外金融要闻速览 ----
    if digest.get('domestic') or digest.get('overseas'):
        lines.append("## 📰 国内外金融要闻速览")
        lines.append("")
        if digest.get('domestic'):
            lines.append("**🇨🇳 国内**")
            lines.extend(f"- {l}" for l in digest['domestic'])
            lines.append("")
        if digest.get('overseas'):
            lines.append("**🌏 海外**")
            lines.extend(f"- {l}" for l in digest['overseas'])
            lines.append("")
        lines.append("---")
        lines.append("")

    for i, a in enumerate(articles, 1):
        ai = a.get('ai', {})
        cats = a['kw']['categories']
        cat_str = "/".join(cats[:3])

        lines.append(f"## {i}. {a['title']}")
        lines.append(f"*{a['source']} | ⏱ 发布于 {fmt_time(a)} (北京时间) | {cat_str} | 相关度 {ai.get('score', '?')}/10*")
        if ai.get('tags'):
            lines.append(f"*{ai['tags']}*")
        lines.append("")
        lines.append(ai.get('summary', '(摘要生成失败)'))
        lines.append("")
        lines.append(f"🔗 [查看原文]({a['link']})")
        lines.append("")
        lines.append("---")
        lines.append("")

    lines.append(f"<small>FinNewsCollectionBot v3.1 | 北京时间 {now.strftime('%H:%M')}</small>")
    return '\n'.join(lines)


def push_wechat(content: str, title: str) -> bool:
    if not SERVER_CHAN_KEYS or not any(k.strip() for k in SERVER_CHAN_KEYS):
        logger.warning("未配置 Server酱 Key，仅打印内容")
        print("\n" + "=" * 60)
        print(content[:3000])
        print("=" * 60)
        return True

    ok = 0
    for key in SERVER_CHAN_KEYS:
        key = key.strip()
        if not key:
            continue
        try:
            r = requests.post(f"https://sctapi.ftqq.com/{key}.send",
                              data={'title': title, 'desp': content}, timeout=20)
            j = r.json()
            if j.get('code') == 0:
                logger.info(f"✓ 微信推送成功 ({key[:12]}...)")
                ok += 1
            else:
                logger.error(f"✗ 推送失败: {j.get('message')}")
        except Exception as e:
            logger.error(f"✗ 推送异常: {e}")
    return ok > 0


# ============================================================
# 入口
# ============================================================
def main():
    test_mode = os.environ.get("TEST_MODE", "false").lower() == "true"
    force = os.environ.get("FORCE_RUN", "false").lower() == "true"

    now = datetime.now(BEIJING_TZ)
    logger.info("=" * 60)
    logger.info(f"FinNewsCollectionBot v3.1 | 北京时间 {now.strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info(f"测试模式: {test_mode} | 强制运行: {force}")
    logger.info("=" * 60)

    state = load_state()

    # ---- 时间守卫 ----
    if not test_mode and not force:
        should_run, period = check_time_window()
        if not should_run:
            logger.info(f"⏰ 当前 {now.strftime('%H:%M')} 不在推送窗口内，跳过")
            logger.info(f"   早报窗口: {MORNING_WINDOW[0]:02d}:{MORNING_WINDOW[1]:02d}-{MORNING_WINDOW[2]:02d}:{MORNING_WINDOW[3]:02d}")
            logger.info(f"   晚报窗口: {EVENING_WINDOW[0]:02d}:{EVENING_WINDOW[1]:02d}-{EVENING_WINDOW[2]:02d}:{EVENING_WINDOW[3]:02d}")
            return

        today = now.strftime('%Y-%m-%d')
        already = state.get(f'{period}_date', '')
        if already == today:
            logger.info(f"✅ 今日{period}已推送完成，跳过避免重复")
            return
        logger.info(f"进入推送流程: {period}")
    else:
        period = 'morning' if now.hour < 12 else 'evening'

    # ---- 抓取 ----
    logger.info("\n📡 [1/4] 抓取 RSS...")
    articles = fetch_rss_feeds()
    if not articles:
        logger.error("未获取到任何文章")
        return

    # ---- 筛选 + AI ----
    logger.info("\n🔍 [2/4] 筛选 + AI 摘要...")
    picked, digest_pool = process(articles, state)

    if not picked:
        logger.warning("无文章通过筛选")
        if test_mode:
            print("（测试模式：无结果，不发推送）")
        return

    # ---- 要闻速览提炼 ----
    logger.info("\n📰 [3/4] 提炼国内外金融要闻速览...")
    digest = ai_digest(digest_pool)

    # ---- 推送 ----
    logger.info("\n📤 [4/4] 推送...")
    content = format_push(picked, period, digest)
    period_cn = "早报" if period == 'morning' else "晚报"
    title = f"🌍 财经要闻{period_cn} · {len(picked)}条 ({now.strftime('%m/%d')})"

    if not test_mode:
        if push_wechat(content, title):
            state[f'{period}_date'] = now.strftime('%Y-%m-%d')
            save_state(state)
            logger.info(f"✅ 推送完成，共 {len(picked)} 条")
        else:
            logger.error("❌ 推送失败，不更新状态")
    else:
        logger.info("[测试模式] 内容预览:")
        print("\n" + "=" * 60)
        print(content)
        print("=" * 60)


if __name__ == "__main__":
    main()

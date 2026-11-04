#!/usr/bin/env python3
"""
FinNewsCollectionBot v3.0 - 期货财经新闻智能聚合系统
========================================================
核心改进 (v3.0):
1. ✅ 时间守卫 + 高频轮询: 北京时间 07:30 / 19:30 精准推送
2. ✅ 双层去重: 批内去重 + 跨批次去重 (3天内不重复)
3. ✅ 全品种覆盖: 能源/贵金属/有色/黑色/谷物/油脂/软商品/航运/化工/农副
4. ✅ 专业数据源: Mining.com / OilPrice / WorldGrain / HellenicShipping 等
5. ✅ 智谱 GLM-4 精要摘要 (200-300字，数据类提炼要点)
6. ✅ Server酱 微信推送 + 品种分布统计
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

# Server酱
SERVER_CHAN_KEYS = os.environ.get("SERVER_CHAN_KEYS", "").split(",")

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
    "CNBC Finance": {
        "url": "https://search.cnbc.com/rs/search/combinedcms/view.xml?partnerId=wrss01&id=10000664",
        "priority": 7, "category": "金融"
    },
    "CNBC World": {
        "url": "https://search.cnbc.com/rs/search/combinedcms/view.xml?partnerId=wrss01&id=100727362",
        "priority": 7, "category": "地缘"
    },
    "MarketWatch Top": {
        "url": "https://feeds.marketwatch.com/marketwatch/topstories/",
        "priority": 6, "category": "金融"
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
        "priority": 8, "category": "金融"
    },
    "Reuters Business": {
        "url": "https://news.google.com/rss/search?q=site:reuters.com+(commodities+OR+oil+OR+metals+OR+grain)&hl=en-US&gl=US&ceid=US:en",
        "priority": 8, "category": "商品综合"
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
    ],
}

# 权重：品种关键词 > 地缘 > 宏观
CATEGORY_WEIGHTS = {
    "能源": 3.0, "贵金属": 3.0, "有色金属": 3.0, "黑色金属": 3.0,
    "谷物": 3.0, "油脂油料": 3.0, "软商品": 3.0, "农副产品": 3.0,
    "化工": 3.0, "航运": 3.0,
    "地缘": 2.0, "宏观金融": 1.5,
}


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
def get_article_full_text(url: str, timeout: int = 12) -> Optional[str]:
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

    prompt = f"""你是国际大宗商品与宏观策略分析师。阅读下面这篇英文新闻，输出中文精要。

【已识别的品种标签】: {', '.join(cats) if cats else '待判断'}

要求：
1. 相关性评分 1-10（10=对商品期货/宏观交易极重要）
2. 如果与以下领域**完全无关**（纯科技产品发布、娱乐八卦、体育赛事、社会轶事），只输出 SKIP
3. 输出 200-350 字中文精要，必须包含：
   - 【核心事实】发生了什么（数字、主体、时间）
   - 【市场影响】对哪些品种/资产有影响，方向（利多/利空）
   - 若原文含关键数据（价格、库存、产量、持仓、百分比），用"· "逐条列出
4. 不要逐句翻译，要提炼。面向专业投资者，语言精炼。

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
def process(articles: list, state: dict) -> list:
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

    # ---- 5. 更新状态 ----
    for art in picked:
        state['hashes'][art['hash']] = now_bj.isoformat()
        ev = art['ai'].get('event', '')
        if ev:
            state['events'].append({'key': ev, 'ts': now_bj.isoformat()})
    state = prune_hashes(state)

    return picked


# ============================================================
# 推送格式
# ============================================================
def format_push(articles: list, period: str) -> str:
    now = datetime.now(BEIJING_TZ)
    period_cn = "早报" if period == 'morning' else "晚报"

    # 品种分布统计
    dist = {}
    for a in articles:
        for c in a['kw']['categories']:
            dist[c] = dist.get(c, 0) + 1
    # 按数量排序
    dist_line = " | ".join(f"{k} {v}" for k, v in sorted(dist.items(), key=lambda x: -x[1])[:10])

    lines = [
        f"# 🌍 国际财经·期货要闻 {period_cn}",
        f"> **{now.strftime('%Y年%m月%d日 %H:%M')}** | 共 **{len(articles)}** 条",
        "",
        f"**品种分布**: {dist_line}",
        "",
        "---",
        "",
    ]

    for i, a in enumerate(articles, 1):
        ai = a.get('ai', {})
        cats = a['kw']['categories']
        cat_str = "/".join(cats[:3])

        lines.append(f"## {i}. {a['title']}")
        lines.append(f"*{a['source']} | {cat_str} | 相关度 {ai.get('score', '?')}/10*")
        if ai.get('tags'):
            lines.append(f"*{ai['tags']}*")
        lines.append("")
        lines.append(ai.get('summary', '(摘要生成失败)'))
        lines.append("")
        lines.append(f"🔗 [查看原文]({a['link']})")
        lines.append("")
        lines.append("---")
        lines.append("")

    lines.append(f"<small>FinNewsCollectionBot v3.0 | 北京时间 {now.strftime('%H:%M')}</small>")
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
    logger.info(f"FinNewsCollectionBot v3.0 | 北京时间 {now.strftime('%Y-%m-%d %H:%M:%S')}")
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
    logger.info("\n📡 [1/3] 抓取 RSS...")
    articles = fetch_rss_feeds()
    if not articles:
        logger.error("未获取到任何文章")
        return

    # ---- 筛选 + AI ----
    logger.info("\n🔍 [2/3] 筛选 + AI 摘要...")
    picked = process(articles, state)

    if not picked:
        logger.warning("无文章通过筛选")
        if test_mode:
            print("（测试模式：无结果，不发推送）")
        return

    # ---- 推送 ----
    logger.info("\n📤 [3/3] 推送...")
    content = format_push(picked, period)
    period_cn = "早报" if period == 'morning' else "晚报"
    title = f"🌍 期货要闻{period_cn} · {len(picked)}条 ({now.strftime('%m/%d')})"

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

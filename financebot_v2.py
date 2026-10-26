#!/usr/bin/env python3
"""
FinNewsCollectionBot v2.1 - 期货财经新闻智能聚合系统
========================================================
核心改进:
1. ✅ 只用免费可访问的数据源 (解决链接打不开问题)
2. ✅ 全文爬取 + 完整翻译 (解决断章取义问题)
3. ✅ 智能筛选: 商品期货 + 地缘政治 + 重大金融
4. ✅ 期货侧重: 金银铜/原油/化工/粮食 优先展示
5. ✅ 智谱 GLM-4 AI 全文翻译 + 相关性判断 (免费)
6. ✅ Server酱 推送完整中文内容
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
from urllib.parse import urlparse, urljoin
from urllib.error import URLError, HTTPError

import feedparser
import requests
from bs4 import BeautifulSoup

# ============================================================
# 配置区
# ============================================================

# 时区设置
BEIJING_TZ = timezone(timedelta(hours=8))

# 智谱 AI API 配置 (OpenAI 兼容接口，免费额度)
OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY", "")
OPENAI_BASE_URL = os.environ.get("OPENAI_BASE_URL", "https://open.bigmodel.cn/api/paas/v4")
OPENAI_MODEL = os.environ.get("OPENAI_MODEL", "glm-4-flash")  # 免费模型

# Server酱配置
SERVER_CHAN_KEYS = os.environ.get("SERVER_CHAN_KEYS", "").split(",")

# 日志配置
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

# ============================================================
# 数据源配置 - 只保留免费可访问的源
# ============================================================
RSS_SOURCES = {
    # === 贵金属/商品专精源 (高优先级) ===
    "Kitco News": {
        "url": "https://www.kitco.com/news/rss",
        "priority": 10,
        "category": "commodity",
        "description": "贵金属与商品市场专业资讯"
    },
    "Kitco Gold & Silver": {
        "url": "https://www.kitco.com/gold-price-today-usa/",
        "priority": 10,
        "category": "precious_metal",
        "description": "金银价格与分析"
    },
    "Investing.com Commodities": {
        "url": "https://www.investing.com/rss/news_301.rss",
        "priority": 9,
        "category": "commodity",
        "description": "商品期货新闻"
    },
    "Investing.com Gold": {
        "url": "https://www.investing.com/rss/news_8830.rss",
        "priority": 9,
        "category": "gold",
        "description": "黄金新闻"
    },
    "Investing.com Oil": {
        "url": "https://www.investing.com/rss/news_8849.rss",
        "priority": 9,
        "category": "oil",
        "description": "原油新闻"
    },
    "Investing.com Silver": {
        "url": "https://www.investing.com/rss/news_8834.rss",
        "priority": 9,
        "category": "silver",
        "description": "白银新闻"
    },
    "Investing.com Copper": {
        "url": "https://www.investing.com/rss/news_8835.rss",
        "priority": 9,
        "category": "base_metal",
        "description": "铜/基本金属新闻"
    },
    "Investing.com Natural Gas": {
        "url": "https://www.investing.com/rss/news_8850.rss",
        "priority": 8,
        "category": "energy",
        "description": "天然气新闻"
    },

    # === 综合财经源 (高质量) ===
    "Reuters Business": {
        "url": "http://feeds.reuters.com/reuters/businessNews",
        "priority": 7,
        "category": "general",
        "description": "路透社商业新闻"
    },
    "Reuters Markets": {
        "url": "http://feeds.reuters.com/reuters/marketsUS",
        "priority": 8,
        "category": "markets",
        "description": "路透社市场新闻"
    },
    "Reuters World": {
        "url": "http://feeds.reuters.com/reuters/worldNews",
        "priority": 6,
        "category": "world",
        "description": "路透社国际新闻"
    },
    "CNBC Markets": {
        "url": "https://search.cnbc.com/rs/search/combinedcms/view.xml?partnerId=wrss01&id=100003114",
        "priority": 7,
        "category": "markets",
        "description": "CNBC 市场新闻"
    },
    "CNBC World": {
        "url": "https://search.cnbc.com/rs/search/combinedcms/view.xml?partnerId=wrss01&id=100727362",
        "priority": 6,
        "category": "world",
        "description": "CNBC 全球新闻"
    },
    "CNBC Energy": {
        "url": "https://search.cnbc.com/rs/search/combinedcms/view.xml?partnerId=wrss01&id=100003113",
        "priority": 8,
        "category": "energy",
        "description": "CNBC 能源新闻"
    },

    # === 另类观点/宏观 ===
    "ZeroHedge": {
        "url": "https://www.zerohedge.com/rss",
        "priority": 6,
        "category": "alternative",
        "description": "另类宏观分析"
    },

    # === Yahoo Finance (免费) ===
    "Yahoo Finance Markets": {
        "url": "https://finance.yahoo.com/news/rssindex",
        "priority": 6,
        "category": "markets",
        "description": "雅虎财经市场新闻"
    },
    "Yahoo Finance Commodities": {
        "url": "https://finance.yahoo.com/news/commodities/",
        "priority": 7,
        "category": "commodity",
        "description": "雅虎财经商品新闻"
    },

    # === MarketWatch (免费) ===
    "MarketWatch Commodities": {
        "url": "https://www.marketwatch.com/rss/story/rss?scid=mkw-commodities",
        "priority": 7,
        "category": "commodity",
        "description": "市场观察商品新闻"
    },
    "MarketWatch Markets": {
        "url": "https://www.marketwatch.com/rss/story/rss?scid=mkw-markets",
        "priority": 6,
        "category": "markets",
        "description": "市场观察市场新闻"
    },

    # === FX Empire (商品专精，免费) ===
    "FX Empire Gold": {
        "url": "https://www.fxempire.com/feeds/gold",
        "priority": 8,
        "category": "gold",
        "description": "黄金分析"
    },
    "FX Empire Commodities": {
        "url": "https://www.fxempire.com/feeds/commodities",
        "priority": 8,
        "category": "commodity",
        "description": "商品分析"
    },
}

# ============================================================
# 关键词筛选体系
# ============================================================

# 商品期货关键词 (最高优先级)
COMMODITY_KEYWORDS = set([
    # 贵金属
    'gold', 'silver', 'platinum', 'palladium', 'precious metal', 'precious metals',
    # 基本金属
    'copper', 'aluminum', 'zinc', 'nickel', 'tin', 'lead', 'base metal', 'base metals',
    # 能源
    'crude oil', 'brent crude', 'wti', 'west texas', 'natural gas', 'oil price',
    'opec', 'opec+', 'saudi arabia oil', 'shale oil',
    # 农产品
    'wheat', 'corn', 'soybean', 'soybeans', 'coffee', 'cotton', 'sugar', 'rice',
    'grain', 'agricultural', 'livestock', 'cattle', 'hog',
    # 化工
    'chemical', 'petrochemical', 'polyethylene', 'pp', 'pvc', 'methanol',
    # 通用商品术语
    'commodity', 'commodities', 'futures contract', 'futures market',
    'lme', 'comex', 'nymex', 'cbot', 'ice', 'dce', 'zce', 'ine',
    'spot price', 'forward curve', 'contango', 'backwardation',
    'inventory level', 'production cut', 'supply chain', 'demand destruction',
])

# 地缘政治关键词 (高优先级)
GEOPOLITICS_KEYWORDS = set([
    'geopolitical', 'geopolitics', 'sanction', 'sanctions', 'trade war',
    'tariff', 'tariffs', 'embargo', 'export ban', 'import duty',
    'middle east', 'iran', 'israel', 'gaza', 'lebanon',
    'ukraine', 'russia', 'putin', 'zelenskyy',
    'china-us', 'sino-us', 'taiwan strait', 'south china sea',
    'north korea', 'kim jong',
    'brics', 'global south', 'dedollarization',
])

# 重大金融关键词 (中优先级)
FINANCE_KEYWORDS = set([
    'federal reserve', 'fed', 'jerome powell', 'interest rate', 'rate hike', 'rate cut',
    'ecb', 'christine lagarde', 'boj', 'bank of japan', 'pboc',
    'central bank', 'monetary policy', 'quantitative easing', 'qt', 'tapering',
    'inflation', 'cpi', 'ppi', 'deflation', 'stagflation', 'recession',
    'gdp', 'employment', 'nonfarm payroll', 'jobless claims',
    'us dollar', 'dxy', 'euro', 'yen', 'yuan', 'currency',
    'bond yield', 'treasury', '10-year', '2-year',
    'stock market', 's&p 500', 'nasdaq', 'dow jones', 'vix',
    'banking crisis', 'credit suisse', 'silicon valley bank',
    'crypto', 'bitcoin', 'ethereum', 'stablecoin', 'sec crypto',
])


def get_article_full_text(url: str, timeout: int = 15) -> Optional[str]:
    """
    爬取文章全文内容
    使用多种策略提取正文
    """
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.9,zh-CN;q=0.8,zh;q=0.7',
    }

    try:
        resp = requests.get(url, headers=headers, timeout=timeout, allow_redirects=True)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, 'html.parser')

        # 移除不需要的标签
        for tag in soup(['script', 'style', 'nav', 'footer', 'header', 'aside', 'iframe', 'noscript']):
            tag.decompose()

        # 策略1: JSON-LD articleBody
        json_ld = soup.find('script', type='application/ld+json')
        if json_ld:
            try:
                data = json.loads(json_ld.string)
                if isinstance(data, dict) and 'articleBody' in data:
                    return data['articleBody']
                elif isinstance(data, list):
                    for item in data:
                        if isinstance(item, dict) and 'articleBody' in item:
                            return item['articleBody']
            except (json.JSONDecodeError, TypeError):
                pass

        # 策略2: 常见文章容器选择器
        selectors = [
            ('article', {}),
            ('[itemprop="articleBody"]', {}),
            ('.article-body', {}),
            ('.article-content', {}),
            ('.post-content', {}),
            ('.entry-content', {}),
            ('.story-body', {}),
            ('.news-content', {}),
            ('main', {}),
            ('#article-body', {}),
            ('#content', {}),
        ]

        for selector, _ in selectors:
            elem = soup.select_one(selector)
            if elem and len(elem.get_text(strip=True)) > 200:
                text = elem.get_text(separator='\n', strip=True)
                # 清理多余空白
                lines = [line.strip() for line in text.split('\n') if line.strip()]
                return '\n'.join(lines)

        # 策略3: 提取所有段落
        paragraphs = soup.find_all('p')
        if paragraphs:
            texts = []
            for p in paragraphs:
                text = p.get_text(strip=True)
                if len(text) > 50:  # 过滤太短的段落
                    texts.append(text)
            if texts and len(' '.join(texts)) > 200:
                return '\n'.join(texts)

        # 最后兜底: 返回 body 文本
        body = soup.find('body')
        if body:
            return body.get_text(separator='\n', strip=True)[:5000]

        return None

    except Exception as e:
        logger.warning(f"爬取文章失败 [{url[:80]}]: {e}")
        return None


def keyword_score(title: str, content: str = "") -> dict:
    """
    计算文章的关键词匹配分数
    返回: {'score': int, 'matched_keywords': list, 'categories': list}
    """
    title_lower = title.lower()
    content_lower = (title + " " + content).lower()

    result = {
        'score': 0,
        'matched_commodity': [],
        'matched_geopolitics': [],
        'matched_finance': [],
        'categories': [],
    }

    # 商品期货匹配 (权重 x3)
    for kw in COMMODITY_KEYWORDS:
        if kw in content_lower:
            result['score'] += 3
            if kw not in result['matched_commodity']:
                result['matched_commodity'].append(kw)

    # 地缘政治匹配 (权重 x2.5)
    for kw in GEOPOLITICS_KEYWORDS:
        if kw in content_lower:
            result['score'] += 2.5
            if kw not in result['matched_geopolitics']:
                result['matched_geopolitics'].append(kw)

    # 金融匹配 (权重 x1.5)
    for kw in FINANCE_KEYWORDS:
        if kw in content_lower:
            result['score'] += 1.5
            if kw not in result['matched_finance']:
                result['matched_finance'].append(kw)

    # 标题中的关键词额外加分
    for kw in COMMODITY_KEYWORDS | GEOPOLITICS_KEYWORDS:
        if kw in title_lower:
            result['score'] += 2  # 标题命中加分更多

    # 分类标记
    if result['matched_commodity']:
        result['categories'].append('商品期货')
    if result['matched_geopolitics']:
        result['categories'].append('地缘政治')
    if result['matched_finance']:
        result['categories'].append('金融市场')

    return result


def ai_filter_and_translate(article: dict, max_retries: int = 2) -> Optional[dict]:
    """
    使用智谱 GLM-4 AI 进行:
    1. 判断文章是否与期货/地缘/金融相关
    2. 如果相关，翻译成完整中文
    """
    if not OPENAI_API_KEY:
        logger.warning("未配置 OPENAI_API_KEY，跳过 AI 处理")
        return None

    title = article.get('title', '')
    content = article.get('full_text', '') or article.get('summary', '')
    url = article.get('link', '')
    source = article.get('source', '')

    # 截断过长内容
    max_input_chars = 6000
    if len(content) > max_input_chars:
        content = content[:max_input_chars] + "\n...(内容过长已截断)"

    prompt = f"""你是一个专业的国际财经新闻分析师，专注于商品期货、地缘政治和金融市场分析。

请对以下英文新闻文章进行两步处理:

## 第一步: 相关性判断
判断这篇文章是否与以下领域**相关**（不需要高度相关，只要有一定关联即可）:
- 商品期货(金银铜、原油、天然气、农产品、化工品等)
- 国际地缘政治(可能影响市场的重大事件)
- 重大金融市场动态(央行政策、通胀数据、经济衰退风险、汇率波动等)
- 全球宏观经济(贸易数据、就业数据、PMI等)

只有当文章明显是以下内容时才回复 SKIP:
- 纯科技产品发布/评测
- 娱乐/体育/明星八卦
- 纯本地/社区新闻
- 与经济金融完全无关的内容

**注意：请尽量宽松判断，宁可多推送也不要漏掉重要信息。**

## 第二步: 如果相关，请完成以下任务
1. **相关性评分**: 给出 1-10 分 (10分表示高度相关且重要)
2. **重要性标签**: 从以下标签中选择适用的(可多选):
   🔥 重要 | 📈 利多 | 📉 利空 | ⚠️ 风险 | 🌍 地缘 | 💰 期货 | 🏦 央行
3. **完整中文翻译**: 将文章**完整翻译成流畅的中文**，保持原文的段落结构和信息完整性。
   不要省略任何关键信息，不要只翻译摘要。这是给专业投资者看的日报素材。

---

文章标题: {title}
来源: {source}
链接: {url}

文章正文:
{content}

请按以下格式输出(如果相关):
---
【相关性评分】: X/10
【重要性标签】: 标签列表
【中文翻译】:
(完整的中文翻译内容)
---

如果不相关，只需输出: SKIP"""

    headers = {
        'Authorization': f'Bearer {OPENAI_API_KEY}',
        'Content-Type': 'application/json',
    }

    for attempt in range(max_retries + 1):
        try:
            resp = requests.post(
                f"{OPENAI_BASE_URL}/chat/completions",
                headers=headers,
                json={
                    "model": OPENAI_MODEL,
                    "messages": [
                        {"role": "system", "content": "你是专业的国际财经新闻分析师和翻译专家。"},
                        {"role": "user", "content": prompt}
                    ],
                    "temperature": 0.3,
                    "max_tokens": 4000,
                },
                timeout=60
            )
            resp.raise_for_status()
            data = resp.json()
            ai_response = data['choices'][0]['message']['content'].strip()

            # 检查是否被跳过
            if ai_response.upper().startswith('SKIP'):
                logger.info(f"AI 判定不相关: {title[:50]}")
                return None

            # 解析 AI 输出
            score_match = re.search(r'【相关性评分】\s*:\s*(\d+)/?10?', ai_response)
            tags_match = re.search(r'【重要性标签】\s*:\s*(.+)', ai_response)
            translate_match = re.search(r'【中文翻译】\s*:\s*\n?(.*)', ai_response, re.DOTALL)

            return {
                'ai_score': int(score_match.group(1)) if score_match else 5,
                'ai_tags': tags_match.group(1).strip() if tags_match else '',
                'chinese_translation': translate_match.group(1).strip() if translate_match else ai_response,
                'raw_response': ai_response,
            }

        except requests.exceptions.Timeout:
            logger.warning(f"AI 调用超时 ({attempt+1}/{max_retries+1}): {title[:40]}")
            if attempt < max_retries:
                time.sleep(3)
        except Exception as e:
            logger.warning(f"AI 调用失败 ({attempt+1}/{max_retries+1}): {e}")
            if attempt < max_retries:
                time.sleep(3)

    return None


def fetch_rss_feeds() -> list:
    """
    抓取所有 RSS 源的文章
    返回: [{'title', 'link', 'summary', 'source', 'published', 'priority'}, ...]
    """
    articles = []
    seen_urls = set()

    for source_name, source_config in RSS_SOURCES.items():
        url = source_config['url']
        priority = source_config['priority']
        category = source_config.get('category', 'general')

        try:
            logger.info(f"正在抓取: {source_name} ...")
            feed = feedparser.parse(url)

            if not feed.entries:
                logger.warning(f"{source_name}: 未获取到文章")
                continue

            count = 0
            for entry in feed.entries[:20]:  # 每个源最多取20篇（原来15篇）
                link = entry.get('link', '')

                # 去重
                if link in seen_urls:
                    continue
                seen_urls.add(link)

                # 解析发布时间
                published = None
                if hasattr(entry, 'published_parsed') and entry.published_parsed:
                    published = datetime(*entry.published_parsed[:6], tzinfo=timezone.utc)
                elif hasattr(entry, 'updated_parsed') and entry.updated_parsed:
                    published = datetime(*entry.updated_parsed[:6], tzinfo=timezone.utc)

                # 只保留最近48小时的文章
                if published:
                    age_hours = (datetime.now(timezone.utc) - published).total_seconds() / 3600
                    if age_hours > 48:
                        continue

                articles.append({
                    'title': entry.get('title', '').strip(),
                    'link': link,
                    'summary': entry.get('summary', '') or entry.get('description', ''),
                    'source': source_name,
                    'category': category,
                    'priority': priority,
                    'published': published,
                })
                count += 1

            logger.info(f"{source_name}: 获取 {count} 篇文章")

        except Exception as e:
            logger.error(f"抓取失败 [{source_name}]: {e}")

    logger.info(f"共获取 {len(articles)} 篇原始文章")
    return articles


def process_articles(articles: list, test_mode: bool = False) -> list:
    """
    处理流程:
    1. 关键词初筛
    2. 爬取全文
    3. AI 判断 + 翻译
    4. 按相关性排序
    """
    results = []

    # 第一步: 关键词初筛
    logger.info("=" * 50)
    logger.info("第一步: 关键词初筛...")
    scored_articles = []

    for article in articles:
        score_info = keyword_score(article['title'], article.get('summary', ''))

        # 设置阈值: 分数 >= 1.5 就进入下一步 (降低门槛，让更多文章进入AI筛选)
        if score_info['score'] >= 1.5:
            article['keyword_score'] = score_info
            scored_articles.append(article)
            logger.debug(f"✓ 通过初筛 [{score_info['score']:.0f}分]: {article['title'][:50]}")
        else:
            logger.debug(f"✗ 未通过初筛 [{score_info['score']:.0f}分]: {article['title'][:50]}")

    logger.info(f"初筛后剩余 {len(scored_articles)} 篇 (原始 {len(articles)} 篇)")

    # 按 keyword_score 排序，优先处理高分文章
    scored_articles.sort(key=lambda x: x['keyword_score']['score'], reverse=True)

    # 限制处理数量 (API 成本控制) - 提高到35篇
    max_process = min(len(scored_articles), 35) if not test_mode else min(len(scored_articles), 8)
    to_process = scored_articles[:max_process]

    # 第二步: 爬取全文 + AI 处理
    logger.info("=" * 50)
    logger.info(f"第二步: 爬取全文 + AI 分析翻译 (处理前 {len(to_process)} 篇)...")

    for i, article in enumerate(to_process):
        title = article['title']
        link = article['link']
        logger.info(f"\n[{i+1}/{len(to_process)}] 处理: {title[:60]}...")

        # 爬取全文
        full_text = get_article_full_text(link)
        if full_text:
            article['full_text'] = full_text
            logger.info(f"  ✓ 全文获取成功 ({len(full_text)} 字符)")
        else:
            logger.warning(f"  ⚠ 全文获取失败，使用摘要代替")
            article['full_text'] = article.get('summary', '')

        # AI 判断 + 翻译
        ai_result = ai_filter_and_translate(article)
        if ai_result:
            article['ai_result'] = ai_result
            final_score = (
                article['keyword_score']['score'] * 0.4 +  # 关键词分占40%
                ai_result['ai_score'] * 0.6                  # AI评分占60%
            )
            article['final_score'] = final_score
            results.append(article)
            logger.info(f"  ✓ AI 通过 (评分:{ai_result['ai_score']}/10, 最终:{final_score:.1f})")
        else:
            logger.info(f"  ✗ AI 判定不相关或处理失败，跳过")

    # 按最终分数排序
    results.sort(key=lambda x: x.get('final_score', 0), reverse=True)

    logger.info("\n" + "=" * 50)
    logger.info(f"最终筛选出 {len(results)} 篇高质量文章")

    return results


def format_push_content(articles: list, test_mode: bool = False) -> str:
    """
    格式化推送内容为中文 Markdown
    """
    now_beijing = datetime.now(BEIJING_TZ)
    time_str = now_beijing.strftime("%Y年%m月%d日 %H:%M")
    period = "早报" if now_beijing.hour < 12 else "晚报"

    lines = [
        f"# 🌍 国际财经·期货要闻{period}",
        f"> **{time_str}** | FinNewsCollectionBot v2.0",
        "",
        f"本期共筛选 **{len(articles)}** 条重要资讯",
        "",
        "---",
        "",
    ]

    for i, article in enumerate(articles[:20]):  # 最多推送20条（原来15条）
        ai = article.get('ai_result', {})
        score = ai.get('ai_score', '?')
        tags = ai.get('ai_tags', '')
        translation = ai.get('chinese_translation', '')
        source = article.get('source', '')
        link = article.get('link', '')

        # 标题行
        lines.append(f"## {i+1}. {article['title']}")
        lines.append(f"")

        # 元信息
        meta_parts = [source]
        if tags:
            meta_parts.append(tags)
        meta_parts.append(f"相关度: {score}/10")
        lines.append(f"*{' | '.join(meta_parts)}*")
        lines.append(f"")

        # 中文翻译内容
        if translation:
            # 截断过长的翻译 (Server酱有长度限制)
            if len(translation) > 1500:
                translation = translation[:1500] + "\n...(全文较长，点击链接查看原文)"
            lines.append(translation)
        else:
            lines.append("(翻译内容暂不可用)")

        lines.append("")
        lines.append(f"🔗 [查看原文]({link})")
        lines.append("")
        lines.append("---")
        lines.append("")

    # 底部信息
    lines.append("")
    lines.append("---")
    lines.append(f"<small>由 FinNewsCollectionBot v2.0 自动生成 | 数据源: Reuters/CNBC/Kitco/Investing/ZeroHedge 等</small>")

    return '\n'.join(lines)


def send_to_serverchan(content: str, title: str = "") -> bool:
    """
    通过 Server酱 发送微信推送
    支持多 key 轮发
    """
    if not SERVER_CHAN_KEYS or not any(SERVER_CHAN_KEYS):
        logger.warning("未配置 Server酱 Key，跳过推送")
        print("\n" + "=" * 60)
        print("[测试模式] 推送内容如下:")
        print("=" * 60)
        print(content)
        print("=" * 60)
        return True

    success_count = 0
    for key in SERVER_CHAN_KEYS:
        key = key.strip()
        if not key:
            continue

        url = f"https://sctapi.ftqq.com/{key}.send"

        try:
            resp = requests.post(url, data={
                'title': title or f"📊 期货财经要闻 {datetime.now(BEIJING_TZ).strftime('%m/%d %H:%M')}",
                'desp': content,
            }, timeout=15)

            result = resp.json()
            if result.get('code') == 0:
                logger.info(f"✓ Server酱推送成功 (key: {key[:15]}...)")
                success_count += 1
            else:
                logger.error(f"✗ Server酱推送失败: {result.get('message', '未知错误')}")

        except Exception as e:
            logger.error(f"✗ Server酱发送异常: {e}")

    return success_count > 0


def main(test_mode: bool = False):
    """主函数"""
    start_time = time.time()
    logger.info("=" * 60)
    logger.info("FinNewsCollectionBot v2.0 启动")
    logger.info(f"时间: {datetime.now(BEIJING_TZ).strftime('%Y-%m-%d %H:%M:%S')} 北京时间")
    logger.info(f"测试模式: {test_mode}")
    logger.info("=" * 60)

    # 1. 抓取 RSS
    logger.info("\n📡 [1/3] 抓取 RSS 数据源...")
    articles = fetch_rss_feeds()
    if not articles:
        logger.error("未获取到任何文章，退出")
        return

    # 2. 筛选 + 翻译
    logger.info("\n🔍 [2/3] 智能筛选 + AI 翻译...")
    processed = process_articles(articles, test_mode=test_mode)
    if not processed:
        logger.warning("没有文章通过筛选，可能今天没有重要的期货/地缘相关新闻")
        # 即使没有相关文章也发一条通知
        send_to_serverchan("今日暂无高相关性期货/地缘/金融新闻。\n\n系统正常运行，只是当前新闻与您的关注领域匹配度较低。",
                          f"📊 期货财经要闻 {datetime.now(BEIJING_TZ).strftime('%m/%d %H:%M')} - 无更新")
        return

    # 3. 格式化 + 推送
    logger.info("\n📤 [3/3] 格式化并推送...")
    push_content = format_push_content(processed, test_mode=test_mode)

    now_beijing = datetime.now(BEIJING_TZ)
    push_title = f"📊 期货财经要闻{('早' if now_beijing.hour < 12 else '晚')}报 · {len(processed)}条"

    if send_to_serverchan(push_content, push_title):
        logger.info(f"\n✅ 推送完成! 共 {len(processed)} 条文章")
    else:
        logger.error("\n❌ 推送失败")

    elapsed = time.time() - start_time
    logger.info(f"总耗时: {elapsed:.1f}秒")


if __name__ == "__main__":
    test_mode = os.environ.get("TEST_MODE", "false").lower() == "true"
    main(test_mode=test_mode)

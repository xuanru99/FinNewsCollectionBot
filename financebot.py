# 福生无量天尊
from openai import OpenAI
import feedparser
import requests
from newspaper import Article
from datetime import datetime
import time
import pytz
import os
import traceback

# OpenAI API Key
openai_api_key = os.getenv("OPENAI_API_KEY")
# 从环境变量获取 Server酱 SendKeys
server_chan_keys_env = os.getenv("SERVER_CHAN_KEYS")
if not server_chan_keys_env:
    raise ValueError("环境变量 SERVER_CHAN_KEYS 未设置，请在Github Actions中设置此变量！")
server_chan_keys = server_chan_keys_env.split(",")

# 初始化 OpenAI 客户端（DeepSeek）
if openai_api_key:
    openai_client = OpenAI(api_key=openai_api_key, base_url="https://api.deepseek.com/v1")
else:
    openai_client = None
    print("⚠️ 未设置 OPENAI_API_KEY，AI 摘要功能将禁用")

# RSS源地址列表
rss_feeds = {
    "💲 华尔街见闻":{
        "华尔街见闻":"https://dedicated.wallstreetcn.com/rss.xml",
    },
    "💻 36氪":{
        "36氪":"https://36kr.com/feed",
        },
    "🇨🇳 中国经济": {
        "东方财富":"http://rss.eastmoney.com/rss_partener.xml",
        "百度股票焦点":"http://news.baidu.com/n?cmd=1&class=stock&tn=rss&sub=0",
        "中新网":"https://www.chinanews.com.cn/rss/finance.xml",
        "国家统计局-最新发布":"https://www.stats.gov.cn/sj/zxfb/rss.xml",
    },
    "🇺🇸 美国经济": {
        "ZeroHedge华尔街新闻": "https://feeds.feedburner.com/zerohedge/feed",
        "ETF Trends": "https://www.etftrends.com/feed/",
        "Federal Reserve Board": "https://www.federalreserve.gov/feeds/press_all.xml",
    },
    "🌍 世界经济": {
        "BBC全球经济": "http://feeds.bbci.co.uk/news/business/rss.xml",
        "FT中文网": "https://www.ftchinese.com/rss/feed",
        "Wall Street Journal": "https://feeds.a.dj.com/rss/RSSWorldNews.xml",
        "Investing.com": "https://www.investing.com/rss/news.rss",
        "Thomson Reuters": "https://ir.thomsonreuters.com/rss/news-releases.xml",
    },
}

# 获取北京时间
def today_date():
    return datetime.now(pytz.timezone("Asia/Shanghai")).date()

# 爬取网页正文 (用于 AI 分析，但不展示) - 带超时和错误处理
def fetch_article_text(url):
    try:
        print(f"📰 正在爬取文章内容: {url}")
        article = Article(url)
        article.download(timeout=10)  # 10秒超时
        article.parse()
        text = article.text[:1500]  # 限制长度
        if not text:
            print(f"⚠️ 文章内容为空: {url}")
            return "（文章内容为空）"
        return text
    except Exception as e:
        print(f"⚠️ 文章爬取失败: {url}，错误: {str(e)[:100]}")
        return "（未能获取文章正文）"

# 添加 User-Agent 头并禁用 SSL 验证
def fetch_feed_with_headers(url):
    import ssl as _ssl
    import urllib.request as _urllib
    _headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }
    ssl_context = _ssl.create_default_context()
    ssl_context.check_hostname = False
    ssl_context.verify_mode = _ssl.CERT_NONE
    req = _urllib.Request(url, headers=_headers)
    with _urllib.urlopen(req, context=ssl_context, timeout=15) as response:
        content = response.read()
    return feedparser.parse(content)


# 自动重试获取 RSS
def fetch_feed_with_retry(url, retries=2, delay=3):
    for i in range(retries):
        try:
            feed = fetch_feed_with_headers(url)
            if feed and hasattr(feed, 'entries') and len(feed.entries) > 0:
                return feed
        except Exception as e:
            print(f"⚠️ 第 {i+1} 次请求 {url} 失败: {str(e)[:80]}")
            if i < retries - 1:
                time.sleep(delay)
    print(f"❌ 跳过 {url}, 尝试 {retries} 次后仍失败。")
    return None

# 获取RSS内容（爬取正文但不展示）
def fetch_rss_articles(rss_feeds, max_articles=5):
    news_data = {}
    analysis_text = ""  # 用于AI分析的正文内容

    for category, sources in rss_feeds.items():
        category_content = ""
        for source, url in sources.items():
            print(f"📡 正在获取 {source} 的 RSS 源: {url}")
            try:
                feed = fetch_feed_with_retry(url)
                if not feed:
                    print(f"⚠️ 无法获取 {source} 的 RSS 数据")
                    continue
                print(f"✅ {source} RSS 获取成功，共 {len(feed.entries)} 条新闻")

                articles = []
                for entry in feed.entries[:max_articles]:
                    title = entry.get('title', '无标题')
                    link = entry.get('link', '') or entry.get('guid', '')
                    if not link:
                        continue

                    # 爬取正文用于分析（不展示）
                    article_text = fetch_article_text(link)
                    analysis_text += f"【{title}】\n{article_text}\n\n"

                    print(f"  🔹 {title}")
                    articles.append(f"- [{title}]({link})")

                if articles:
                    category_content += f"### {source}\n" + "\n".join(articles) + "\n\n"
            except Exception as e:
                print(f"❌ {source} 处理异常: {str(e)[:100]}")
                continue

        news_data[category] = category_content

    return news_data, analysis_text

# AI 生成内容摘要（基于爬取的正文）- 带错误处理
def summarize(text):
    if not openai_client:
        return "（AI 摘要功能未启用，请配置 OPENAI_API_KEY）"
    
    if not text or len(text.strip()) < 50:
        return "（新闻内容不足，无法生成 AI 摘要）"
    
    try:
        completion = openai_client.chat.completions.create(
            model="deepseek-chat",
            messages=[
                {"role": "system", "content": """
                 你是一名专业的财经新闻分析师，请根据以下新闻内容，按照以下步骤完成任务：
                 1. 提取新闻中涉及的主要行业和主题，找出近1天涨幅最高的3个行业或主题，以及近3天涨幅较高且此前2周表现平淡的3个行业/主题。（如新闻未提供具体涨幅，请结合描述和市场情绪推测热点）
                 2. 针对每个热点，输出：
                    - 催化剂：分析近期上涨的可能原因（政策、数据、事件、情绪等）。
                    - 复盘：梳理过去3个月该行业/主题的核心逻辑、关键动态与阶段性走势。
                    - 展望：判断该热点是短期炒作还是有持续行情潜力。
                 3. 将以上分析整合为一篇1500字以内的财经热点摘要，逻辑清晰、重点突出，适合专业投资者阅读。
                 """},
                {"role": "user", "content": text[:8000]}  # 限制输入长度
            ],
            timeout=60
        )
        return completion.choices[0].message.content.strip()
    except Exception as e:
        print(f"❌ AI 摘要生成失败: {str(e)[:200]}")
        traceback.print_exc()
        return f"（AI 摘要生成失败: {str(e)[:100]}）"

# 发送微信推送
def send_to_wechat(title, content):
    for key in server_chan_keys:
        try:
            url = f"https://sctapi.ftqq.com/{key}.send"
            data = {"title": title, "desp": content}
            response = requests.post(url, data=data, timeout=15)
            if response.ok:
                print(f"✅ 推送成功")
            else:
                print(f"❌ 推送失败: {response.text[:200]}")
        except Exception as e:
            print(f"❌ 推送异常: {str(e)[:100]}")


if __name__ == "__main__":
    print("=" * 50)
    print(f"🚀 FinNewsCollectionBot 启动")
    print(f"📅 {datetime.now(pytz.timezone('Asia/Shanghai')).strftime('%Y-%m-%d %H:%M:%S')} 北京时间")
    print("=" * 50)

    today_str = today_date().strftime("%Y-%m-%d")

    # 每个网站获取最多 5 篇文章
    print("\n📡 开始抓取 RSS 新闻...")
    articles_data, analysis_text = fetch_rss_articles(rss_feeds, max_articles=5)
    print(f"\n✅ 新闻抓取完成，共获取 {len(analysis_text)} 字符的分析文本")

    # AI生成摘要
    print("\n🤖 正在生成 AI 摘要...")
    summary = summarize(analysis_text)
    print(f"✅ AI 摘要生成完成，长度: {len(summary)} 字符")

    # 生成最终消息
    final_summary = f"📅 **{today_str} 财经新闻摘要**\n\n✍️ **今日分析总结：**\n{summary}\n\n---\n\n"
    for category, content in articles_data.items():
        if content.strip():
            final_summary += f"## {category}\n{content}\n\n"

    # 推送到微信
    print("\n📱 正在推送到微信...")
    send_to_wechat(title=f"📌 {today_str} 财经新闻摘要", content=final_summary)

    # 保存报告文件
    os.makedirs("output", exist_ok=True)
    with open("output/latest_report.txt", "w", encoding="utf-8") as f_txt:
        f_txt.write(final_summary)
    with open("output/latest_report.html", "w", encoding="utf-8") as f_html:
        f_html.write(final_summary.replace("\n", "<br/>"))

    # 归档保存
    beijing_now = datetime.now(pytz.timezone("Asia/Shanghai"))
    ts_str = beijing_now.strftime("%Y%m%d_%H%M%S")
    archive_path = os.path.join("output", f"report_{ts_str}.md")
    with open(archive_path, "w", encoding="utf-8") as f_md:
        f_md.write(final_summary)

    print("\n" + "=" * 50)
    print("✅ FinNewsCollectionBot 运行完成！")
    print("=" * 50)

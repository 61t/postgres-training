import sys
import feedparser
import psycopg2

def fetch_and_store_rss(rss_url):
    # 解析 RSS Feed
    feed = feedparser.parse(rss_url)

    # PostgreSQL 连接配置（根据需要修改）
    db_config = {
        'host': '10.0.2.50',
        'port': 5432,
        'database': 'oracle',
        'user': 'postgres',
        'password': 'db123456'
    }

    # 创建连接
    conn = psycopg2.connect(**db_config)
    cur = conn.cursor()

    # 创建表（如果不存在）
    cur.execute("""
        CREATE TABLE IF NOT EXISTS rss_feed (
            id SERIAL PRIMARY KEY,
            title TEXT,
            link TEXT,
            published TIMESTAMP,
            summary TEXT
        )
    """)
    conn.commit()

    # 插入数据
    for entry in feed.entries:
        cur.execute("""
            INSERT INTO rss_feed (title, link, published, summary)
            VALUES (%s, %s, %s, %s)
        """, (
            entry.get('title'),
            entry.get('link'),
            entry.get('published'),
            entry.get('summary')
        ))

    conn.commit()
    cur.close()
    conn.close()
    print(f"插入 {len(feed.entries)} 条 RSS 数据成功。")

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("用法: python rss_to_pg.py <RSS_URL>")
        sys.exit(1)

    rss_url = sys.argv[1]
    fetch_and_store_rss(rss_url)


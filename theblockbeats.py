import concurrent.futures
import json
import logging
from datetime import datetime

from bs4 import BeautifulSoup
from pymysql.cursors import DictCursor
import pymysql
import requests
from tqdm import tqdm

from hotspot_topic_agent_en import HotspotTopicAnalyzerEN
from utils import load_failed_urls, save_failed_urls, PROXY_POOL, get_random_user_agent
from tabulate import tabulate

import threading

# —— 全局缓冲区与锁 ——
_insert_buffer = []          # 缓存待写入的记录
_buffer_lock = threading.Lock()
BATCH_SIZE = 10            # 达到 10 条时批量写入



ALI_DB_CONFIG = {
    "host": "",
    "user": "",
    "password": "%",
    "db": "",
    "charset": "utf8mb4",
    "cursorclass": DictCursor
}


BASE_URL = "https://en.theblockbeats.news/flash/{}"
END_ID  = 297293
START_ID = 290000
MAX_RETRIES = 2
RETRY_DELAY = 2
FAILED_URLS_FILE = "failed_urls.json"
MAX_WORKERS = len(PROXY_POOL)  # 使用与代理池相同数量的工作线程


# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('techflow_crawler.log'),
        logging.StreamHandler()
    ]
)


# ------------------- 数据库操作 -------------------
def query_db(table_name='theblockbeats_newsletter_news', fields="*", where_clause=None, where_params=None):
    """查询数据库

    Args:
        table_name: 表名，默认为techflow_newsletter_news
        fields: 要查询的字段，默认为*
        where_clause: WHERE条件子句，例如"id = %s AND status = %s"
        where_params: WHERE条件参数列表

    Returns:
        查询结果列表
    """
    try:
        # 连接数据库
        with pymysql.connect(**ALI_DB_CONFIG) as connection:
            with connection.cursor() as cursor:
                # 构建SQL语句
                sql = f"SELECT {fields} FROM {table_name}"

                if where_clause:
                    sql += f" WHERE {where_clause}"

                # 执行查询
                if where_params:
                    cursor.execute(sql, where_params)
                else:
                    cursor.execute(sql)

                records = cursor.fetchall()
                return records
    except Exception as e:
        print(f"查询数据库错误: {e}")
        return []

def get_max_news_id(table_name='theblockbeats_newsletter_news'):
    conn = pymysql.connect(**ALI_DB_CONFIG)
    with conn.cursor() as cursor:
        cursor.execute(f"SELECT MAX(news_id) AS max_id FROM {table_name}")
        row = cursor.fetchone()
    conn.close()
    return row['max_id'] or 0

def update_news_by_id(news_id, update_data, table_name):
    set_clause = ", ".join([f"{key} = %s" for key in update_data.keys()])
    params = list(update_data.values())
    params.append(news_id)  # 最后一个参数是WHERE条件中的ID

    try:
        with pymysql.connect(**ALI_DB_CONFIG) as connection:
            with connection.cursor() as cursor:
                # SQL更新语句
                sql = f"""
                UPDATE {table_name}
                SET {set_clause}
                WHERE id = %s
                """
                cursor.execute(sql, params)
                connection.commit()
                return cursor.rowcount > 0

    except pymysql.Error as e:
        print(f"数据库错误: {e}")
        return False
    except Exception as e:
        print(f"其他错误: {e}")
        return False


# ------------------- 改动：批量写入 save_to_db -------------------
def save_to_db(data):
    """
    将单条数据先放入全局缓冲区 _insert_buffer，
    当缓冲区大小 >= BATCH_SIZE 时，调用 _do_batch_insert_unlocked 批量写入。
    """
    global _insert_buffer

    record = (data['original_url'], data['main_content'], data['news_id'])

    with _buffer_lock:
        _insert_buffer.append(record)
        if len(_insert_buffer) >= BATCH_SIZE:
            _do_batch_insert_unlocked()  # 缓冲区满 100 条时批量写入

# 新增批量写入函数，假定调用时已经拿了 _buffer_lock
def _do_batch_insert_unlocked():
    """
    真正执行批量插入，假定 _buffer_lock 已经被 acquire。
    """
    global _insert_buffer

    batch_rows = _insert_buffer      # 拷贝当前所有记录
    _insert_buffer = []              # 清空全局缓冲区

    if not batch_rows:
        return

    try:
        conn = pymysql.connect(**ALI_DB_CONFIG)
        with conn.cursor() as cursor:
            sql = """
                INSERT INTO theblockbeats_newsletter_news
                    (original_url, main_content, news_id)
                VALUES (%s, %s, %s)
            """
            cursor.executemany(sql, batch_rows)
        conn.commit()
        logging.info(f"批量写入 {len(batch_rows)} 条到数据库")
    except Exception as e:
        logging.error(f"批量插入失败: {e}")
        # 如果需要失败后重试，可以把 batch_rows 放回 _insert_buffer：
        # with _buffer_lock:
        #     _insert_buffer = batch_rows + _insert_buffer
    finally:
        conn.close()

# ------------------- 爬虫核心 -------------------

def generate_urls():

    """生成所有需要爬取的URL"""
    failed_urls = load_failed_urls()
    all_urls = [BASE_URL.format(i) for i in range(START_ID, END_ID + 1)]

    # 将失败的URL移到列表前面
    urls_to_crawl = failed_urls + [url for url in all_urls if url not in failed_urls]
    return urls_to_crawl

def crawl_single_news(url, proxy):
    """爬取单个新闻页面"""
    headers = {
        "User-Agent": get_random_user_agent(),
        "Referer": "https://www.techflowpost.com/"
    }

    try:
        response = requests.get(
            url,
            headers=headers,
            proxies=proxy,
            timeout=10
        )
        response.raise_for_status()
        response.encoding = 'utf-8'

        result = {
            'main_content': response.text,
            'original_url': url,
            'news_id': int(url.split('/')[-1])
        }

        save_to_db(result)
        logging.info(f"成功爬取: {url}")
        return url, True

    except Exception as e:
        logging.error(f"爬取失败: {url} - {str(e)}")
        return url, False

def process_urls_with_proxies(urls):
    """使用所有代理并发处理URL"""
    successful = 0
    failed = []

    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        # 为每个URL分配一个代理
        future_to_url = {
            executor.submit(crawl_single_news, url, proxy): url
            for url, proxy in zip(urls, PROXY_POOL * (len(urls) // len(PROXY_POOL) + 1))
        }

        for future in tqdm(concurrent.futures.as_completed(future_to_url), total=len(urls), desc="爬取进度"):
            url, success = future.result()
            if success:
                successful += 1
            else:
                failed.append(url)

    return successful, failed

def crawl_all_news():
    start_id = get_max_news_id() + 1
    consecutive_failures = 0
    current_id = start_id
    logging.info(f"从 ID={start_id} 开始爬取，连续三次失败则停止")

    while consecutive_failures < 3:
        url = BASE_URL.format(current_id)
        proxy = PROXY_POOL[(current_id - start_id) % len(PROXY_POOL)]
        if crawl_single_news(url, proxy):
            consecutive_failures = 0
        else:
            consecutive_failures += 1
        current_id += 1

    # 最后 flush
    with _buffer_lock:
        _do_batch_insert_unlocked()
    logging.info(f"已连续{consecutive_failures}次失败，停止爬取。最后尝试 ID={current_id-1}")


# ------------------- 执行流程 -------------------

def parse(id,html_content):
    """从HTML内容中提取标题、事件和内容"""
    soup = BeautifulSoup(html_content, 'html.parser')
    news_items = []

    # 定位每个新闻项
    flash_items = soup.find_all('div', class_='flash-top-border')

    for item in flash_items:
        try:
            # 提取标题
            title = item.find('h1', class_='flash-title').text.strip()

            # 提取事件时间
            time_element = item.find('div', class_='item-time')
            time = time_element.text.strip() if time_element else "时间未找到"

            # 提取内容
            content = item.find('div', class_='flash-content').text.strip()

            update_news_by_id(id, {"title": title, "publish_time": time, "content": content},"theblockbeats_newsletter_news")

        except AttributeError:
            # 处理可能的解析错误
            continue

    return news_items

def insert_embedding(source_table="theblockbeats_newsletter_news"):
    """插入嵌入向量到指定表"""
    db_name = 'theblockbeats_newsletter_news'
    fields = 'id, embedding, event'
    where_clause = '''STR_TO_DATE(publish_time, '%Y-%m-%d %H:%i')
       >= '2025-06-01 00:00:00'
       '''
    lines = query_db(table_name=db_name, where_clause=where_clause, fields=fields)
    analyzer = HotspotTopicAnalyzerEN()
    for line in lines:
        if not line or line['event'] is None or len(line['event']) == 0  or line['embedding'] is not None:
            print("error" , line['id'], line['event'])
            continue
        print(line['id'], line['event'])
        embedding = (analyzer.compute_topic_embedding(line['event']))
        update_news_by_id(line['id'], {"embedding": json.dumps(embedding)}, table_name=source_table)


def insert_data():
    db_name = 'theblockbeats_newsletter_news'
    fields = 'id, content, event'
    where_clause = '''STR_TO_DATE(publish_time, '%Y-%m-%d %H:%i')
       >= '2025-06-01 00:00:00'
       '''
    datas = query_db(table_name=db_name, fields=fields, where_clause=where_clause)  # 获取全部数据
    filtered_datas = [
        data for data in datas
        if data.get('content') and
           len(data['content']) > 8 and
           data.get('event') is None
    ]


    analyzer = HotspotTopicAnalyzerEN()
    total = len(filtered_datas)

    # 每10个数据为一组进行处理
    for i in range(0, total, 10):
        batch_datas = filtered_datas[i:i+10]  # 当前批次数据
        news = [data['content'] for data in batch_datas]

        # 分析当前批次新闻
        print(f"处理第 {i//10+1} 组数据，共 {len(news)} 条")
        results = analyzer.analyze_and_store_news(news)

        if len(news) == len(results.flash_analysis):
            for j in range(len(news)):
                result = results.flash_analysis[j].__dict__
                result['key_entities'] = [entity.model_dump() for entity in result['key_entities']]
                result['key_entities'] = json.dumps(result['key_entities'])
                update_news_by_id(batch_datas[j]['id'], result, table_name=db_name)
        else:
            print(f"警告：第 {i//10+1} 组数据处理结果数量不匹配！")


# 批量解析main_content
def process_news_in_batches(batch_size: int = 10):
    """
    将表 theblockbeats_newsletter_news 中的所有记录，按每 batch_size 条一组分批取出，
    并在取完后逐条调用 parse(id, html_content) 处理，同时在控制台显示进度条。
    """
    # 1. 先查总记录数，用于 tqdm 的 total
    try:
        conn = pymysql.connect(**ALI_DB_CONFIG)
        with conn.cursor() as cursor:
            cursor.execute("SELECT COUNT(*) AS cnt FROM theblockbeats_newsletter_news")
            total_count = cursor.fetchone()["cnt"]
        conn.close()
    except Exception as e:
        print(f"[ERROR] 查询总记录数失败：{e}")
        return

    # 2. 初始化一个 tqdm 进度条，总长度设为 total_count
    progress = tqdm(total=total_count, desc="处理新闻记录", unit="条")

    offset = 0
    while True:
        try:
            # 3. 按批次从数据库读取 id 和 main_content
            conn = pymysql.connect(**ALI_DB_CONFIG)
            with conn.cursor() as cursor:
                sql = """
                    SELECT id, main_content
                    FROM theblockbeats_newsletter_news
                    ORDER BY id
                    LIMIT %s OFFSET %s
                """
                cursor.execute(sql, (batch_size, offset))
                rows = cursor.fetchall()
            conn.close()

            # 如果本轮没有数据，说明已经处理完所有行，退出循环
            if not rows:
                break

            # 4. 对这一批里的每一行，调用 parse 并更新进度条
            for row in rows:
                _id = row["id"]
                html = row["main_content"]
                try:
                    parse(_id, html)
                except Exception as e:
                    print(f"[ERROR] 处理 id={_id} 时出错：{e}")
                finally:
                    # 不管成功或失败，都让进度条前进一步
                    progress.update(1)

            # 5. 本批处理完，offset 加 batch_size，继续下一批
            offset += batch_size

        except Exception as e:
            print(f"[ERROR] 批量查询或处理出错: {e}")
            break

    # 6. 循环结束后，关闭进度条
    progress.close()

def update_time_field(table_name='theblockbeats_newsletter_news'):
    """
    用 UNIX_TIMESTAMP(STR_TO_DATE(...)) 把 publish_time 转成秒级时间戳，更新到 time 字段。
    """
    conn = pymysql.connect(**ALI_DB_CONFIG)
    try:
        with conn.cursor() as cursor:
            sql = f"""
                UPDATE {table_name}
                SET `time` = UNIX_TIMESTAMP(
                    STR_TO_DATE(publish_time, '%Y-%m-%d %H:%i')
                )
                WHERE publish_time IS NOT NULL
                  AND publish_time <> '';
            """
            rows = cursor.execute(sql)
        conn.commit()
        print(f"✅ 已更新 {rows} 行的 time 字段")
    finally:
        conn.close()

# ------------------- 数据库操作 -------------------
def sql_test(sql):
    """查询数据库

    Args:
        table_name: 表名，默认为techflow_newsletter_news
        fields: 要查询的字段，默认为*
        where_clause: WHERE条件子句，例如"id = %s AND status = %s"
        where_params: WHERE条件参数列表

    Returns:
        查询结果列表
    """
    try:
        # 连接数据库
        with pymysql.connect(**ALI_DB_CONFIG) as connection:
            with connection.cursor() as cursor:
                records = cursor.execute(sql)
                result = cursor.fetchall()
                return result
    except Exception as e:
        print(f"查询数据库错误: {e}")
        return []

if __name__ == "__main__":
    # pass
    #开始爬  crawl_all_news()
    # crawl_all_news()
    #解析网页 parse()
    # process_news_in_batches(10)

    #llm处理数据  insert_data()
    # insert_data()
    # insert_embedding()

    # news_analysis_data
    # theblockbeats_newsletter_news
    # Tables_in_storyline_test

    # sql = 'ALTER TABLE theblockbeats_newsletter_news ADD COLUMN news_id INT,ADD COLUMN main_content TEXT;'
    # update_time_field()
    sql = '''
    desc event_analysis
    '''
    # print(sql_test(sql))
    print(tabulate(sql_test(sql), headers="keys", tablefmt="pretty"))
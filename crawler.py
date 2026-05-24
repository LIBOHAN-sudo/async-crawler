'''
完整爬虫项目讲解
'''
import asyncio
import aiohttp
import redis
import json
import hashlib
import time
import random
import logging
from bs4 import BeautifulSoup
from datetime import datetime
from logging.handlers import RotatingFileHandler

def set_logging():
    logger = logging.getLogger('Crawler')
    logger.setLevel(logging.DEBUG)

    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)

    file_handler = RotatingFileHandler(
        'total.log',
        maxBytes = 10*1024**2,
        backupCount=5,
        encoding='utf-8'
        )
    file_handler.setLevel(logging.DEBUG)

    error_handler = RotatingFileHandler(
        'error.log',
        maxBytes = 5*1024**2,
        backupCount = 3,
        encoding = 'utf-8'
        )
    error_handler.setLevel(logging.ERROR)

    formatter = logging.Formatter(
        fmt = '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt = '%Y-%m-%d %H:%M:%S'
        )
    console_handler.setFormatter(formatter)
    file_handler.setFormatter(formatter)
    error_handler.setFormatter(formatter)

    logger.addHandler(console_handler)
    logger.addHandler(file_handler)
    logger.addHandler(error_handler)

    return logger
logger = set_logging()
    

USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0.0.0',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 Safari/605.1.15',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/119.0.0.0',
    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/120.0.0.0'
]

class ProductionCrawler:
    def __init__(self):
        self.logger = logging.getLogger('Crawler')
        self.redis = redis.Redis(
            host = 'localhost',
            port = 6379,
            decode_responses = True
            )
        # Redis键名
        self.URL_QUEUE = 'crawler:url_queue'
        self.VISITED_SET = 'crawler:visited'
        self.CONTENT_SET = 'crawler:content_hash'
        self.RESULT_LIST = 'crawler:results'
        self.FAILED_SET = 'crawler:failed'
        self.STATS_HASH = 'crawler:stats'
        self.PROXY_KEY = 'crawler:proxies'

    def get_content_hash(self,title,price):
        """生成内容哈希"""
        return hashlib.md5(f'{title}_{price}'.encode()).hexdigest()

    def add_url(self,url):
        """添加URL到队列（自动去重）"""
        if not self.redis.sismember(self.VISITED_SET,url):
            self.redis.lpush(self.URL_QUEUE,url)
            self.logger.debug(f'{url}已添加至队列')

    def get_url(self):
        """获取待爬URL"""
        return self.redis.rpop(self.URL_QUEUE)

    def add_proxy(self,proxy):
        self.redis.sadd(self.PROXY_KEY,proxy)
        self.logger.debug(f'已添加:{proxy}代理')
        
    def get_proxy(self):
        proxies = list(self.redis.smembers(self.PROXY_KEY))
        return random.choice(proxies) if proxies else None

    def remove_proxy(self,proxy):
        removed = self.redis.srem(self.PROXY_KEY,proxy)
        if removed:
            self.logger.warning(f'已移除失效代理: {proxy}')
        else:
            self.logger.debug(f'代理不存在或已移除: {proxy}')
        
    def mark_visited(self,url):
        """标记已访问"""
        self.redis.sadd(self.VISITED_SET,url)
        self.redis.hincrby(self.STATS_HASH,'visited',1)
        self.logger.debug(f'标记已访问:{url}')

    def mark_failed(self,url,error):
        """标记失败"""
        self.redis.sadd(self.FAILED_SET,url)
        self.redis.hincrby(self.STATS_HASH,'failed',1)
        self.redis.hset(f'error:{url}','error',error)
        self.redis.hset(f'error:{url}','time',time.time())
        self.logger.warning(f'请求失败[{url}]:{error}')

    def save_book(self,book_data):
        # 去重检查
        content_hash = self.get_content_hash(
            book_data.get('title'),
            book_data.get('price')
            )

        result = self.redis.sismember(self.CONTENT_SET,content_hash)

        if result:
            self.redis.hincrby(self.STATS_HASH,'duplicates',1)
            self.logger.debug(f'发现重复数据:{content_hash}')
            return False
        pipe = self.redis.pipeline()
        pipe.sadd(self.CONTENT_SET,content_hash)
        book_data['content_hash'] = content_hash
        book_data['crawled_at'] = datetime.now().isoformat()

        pipe.lpush(self.RESULT_LIST,json.dumps(book_data,ensure_ascii=False))
        pipe.hincrby(self.STATS_HASH,'saved',1)
        pipe.execute()
        #当您使用Redis管道执行多个命令时，execute() 会返回一个列表，
        #包含每个命令的执行结果，按命令添加的顺序排列。
        self.logger.info(f'已保存书籍:{book_data["title"]}')
        return True

    def clear_all_data(self):
        """爬取单页（书籍列表页）"""
        self.logger.info('正在清理历史数据...')
        pipe = self.redis.pipeline()

        keys_to_delete = [
            self.URL_QUEUE,
            self.VISITED_SET,
            self.CONTENT_SET,
            self.RESULT_LIST,
            self.FAILED_SET,
            self.STATS_HASH
            ]
        for key in keys_to_delete:
            pipe.delete(key)

        error_keys = list(self.redis.scan_iter("error:*"))
        for key in error_keys:
            pipe.delete(key)

        pipe.execute()
        self.logger.info(f'清理完成，删除了{len(keys_to_delete)+len(error_keys)}个key')
    
        
    async def close(self):
        self.logger.info('关闭Redis连接')
        self.redis.close()

    async def crawl_book_page(self,session,url,max_retries=3):     
        for i in range(max_retries):
            try:
                headers = {'User-Agent': random.choice(USER_AGENTS)}
                proxy = self.get_proxy()
                async with session.get(url,timeout=15,headers=headers,proxy=proxy) as resp:
                    if resp.status == 200:
                        pass
                    elif resp.status == 429:
                        wait_time = 2**i
                        self.logger.warning(f'遇到限流,等待{wait_time}秒后重试')
                        await asyncio.sleep(wait_time)
                        continue
                    else:
                        self.mark_failed(url,f'HTTP(page) {resp.status}')
                        return []
                    html = await resp.text()
                    soup = BeautifulSoup(html,'lxml')

                    # 解析书籍链接
                    books = soup.find_all('article',class_='product_pod')
                    book_urls = []
                    for book in books:
                        relative_url = book.h3.a['href']
                        full_url = f'https://books.toscrape.com/catalogue/{relative_url.replace("../","")}'
                        book_urls.append(full_url)

                    self.mark_visited(url)
                    return book_urls
            except asyncio.TimeoutError:
                if proxy:
                    self.remove_proxy(proxy)
                self.mark_failed(url,'Timeout')
            except aiohttp.ClientProxyConnectionError:
                if proxy:
                    self.remove_proxy(proxy)
                self.mark_failed(url, 'Proxy connection failed')
            except aiohttp.ClientConnectorError as e:
                if proxy:
                    self.remove_proxy(proxy)
                self.mark_failed(url, str(e))
            except Exception as e:
                self.mark_failed(url,str(e))
        error_msg = self.redis.hget(f'error:{url}', 'error')
        self.logger.error(f'错误信息:{error_msg}')
        return []

    async def crawl_book_detail(self,session,url,max_retries=3):
         """爬取书籍详情页"""
        for i in range(max_retries):
            try:
                headers = {'User-Agent': random.choice(USER_AGENTS)}
                proxy = self.get_proxy()
                async with session.get(url,timeout=10,headers=headers,proxy=proxy) as resp:
                    if resp.status == 200:
                        pass
                    elif resp.status == 429:
                        wait_time = 2**i
                        self.logger.warning(f'遇到限流,等待{wait_time}秒后重试')
                        await asyncio.sleep(wait_time)
                        continue
                    else:
                        self.mark_failed(url,f'HTTP(detail) {resp.status}')
                        return None

                    html = await resp.text()
                    soup = BeautifulSoup(html,'lxml')

                    title = soup.h1.text if soup.h1 else ''
                    
                    price_elem = soup.find('p',class_='price_color')
                    price = price_elem.text if price_elem else ''

                    rating_elem = soup.find('p',class_='star-rating')
                    rating = rating_elem['class'][1] if rating_elem else ''
                    rating_map = {'One':1,'Two':2,'Three':3,'Four':4,'Five':5}
                    rating = rating_map.get(rating,0)

                    stock_elem = soup.find('p',class_='instock availability')
                    stock = stock_elem.text.strip() if stock_elem else ''

                    return {
                        'url':url,
                        'title':title,
                        'price':price,
                        'rating':rating,
                        'stock':stock
                        }
            except asyncio.TimeoutError:
                if proxy:
                    self.remove_proxy(proxy)
                self.mark_failed(url,'Timeout')
            except aiohttp.ClientProxyConnectionError:
                if proxy:
                    self.remove_proxy(proxy)
                self.mark_failed(url, 'Proxy connection failed')
            except aiohttp.ClientConnectorError as e:
                if proxy:
                    self.remove_proxy(proxy)
                self.mark_failed(url, str(e))
            except Exception as e:
                self.mark_failed(url,str(e))
        error_msg = self.redis.hget(f'error:{url}', 'error')
        self.logger.error(f'错误信息:{error_msg}')
        return None

    async def worker(self,worker_id):
        self.logger.info(f"工作线程 {worker_id} 启动")
        async with aiohttp.ClientSession() as session:
            while True:
                url = self.get_url()
                if not url:
                    self.logger.info(f"工作线程 {worker_id} 队列为空，退出")
                    break     
                if '/catalogue/page-' in url:
                    # 列表页：解析出详情页URL
                    book_urls = await self.crawl_book_page(session,url)
                    for book_url in book_urls:
                        self.add_url(book_url)
                else:
                    # 详情页：保存数据
                    book_data = await self.crawl_book_detail(session,url)
                    if book_data:
                        self.save_book(book_data)
                        self.logger.info(f'[{worker_id}] √ {book_data["title"]}')
                    else:
                        self.mark_failed(url,'书本数据获取失败')
                        self.logger.warning('\n'+'>'*7+'有个书本数据获取失败！'+'<'*7)

                await asyncio.sleep(0.5)

    def get_stats(self):
        """获取统计信息"""
        return {
            'queue_size':self.redis.llen(self.URL_QUEUE),
            'visited':int(self.redis.hget(self.STATS_HASH,'visited')or 0),
            'saved':int(self.redis.hget(self.STATS_HASH,'saved')or 0),
            'failed':int(self.redis.hget(self.STATS_HASH, 'failed') or 0),
            'duplicates': int(self.redis.hget(self.STATS_HASH, 'duplicates') or 0)
            }
    def export_results(self,filename='book.json'):
         """导出结果到文件"""
        self.logger.info(f"开始导出数据到 {filename}")
        results=[]
        while True:
            item = self.redis.rpop(self.RESULT_LIST)
            if not item:
                break
            results.append(json.loads(item))

        with open(filename,'w',encoding = 'utf-8') as f:
            json.dump(results,f,ensure_ascii=False,indent=2)

        self.logger.info(f'已导出{len(results)}条数据到{filename}')
        return None

    def export_errors(self,filename='error.json'):
        """导出错误到文件"""
        self.logger.info(f"开始导出错误到 {filename}")
        errors = []
        failed_urls = self.redis.smembers(self.FAILED_SET)
        for url in failed_urls:
            error = self.redis.hget(f'error:{url}','error')
            error_time = self.redis.hget(f'error:{url}','time')
            errors.append({
                'url':url,
                'error':error,
                'time':float(error_time) if error_time else None,
                'timestamp':datetime.fromtimestamp(float(error_time)).isoformat() if error_time else None
                })
        with open(filename,'w',encoding='utf-8') as f:
            json.dump(errors,f,ensure_ascii=False,indent=2)
        self.logger.info(f'已导出{len(errors)}条错误到{filename}')
        return None

async def main():
    logger = logging.getLogger('Crawler')
    logger.info("=" * 50)
    logger.info("爬虫程序启动")
    logger.info("=" * 50)
    crawler = ProductionCrawler()
    crawler.clear_all_data()
    base_urls = [f'https://books.toscrape.com/catalogue/page-{i}.html' for i in range(1, 51)]
    for url in base_urls:
        crawler.add_url(url)
    base_proxies = [
        'http://39.102.214.199:80',
        'http://47.99.112.148:3129',
        'http://47.104.198.111:8008',
        'http://8.219.167.110:8082',
        'http://116.63.130.30:18081'
        ]
    for proxy in base_proxies:
        crawler.add_proxy(proxy)
        
    logger.info(f'已添加{len(base_proxies)}个代理')
    logger.info(f'已添加{len(base_urls)}个初始URL')

    workers = [crawler.worker(i+1) for i in range(5)]

    # 每10秒打印统计
    async def print_stats():
        while True:
            await asyncio.sleep(10)
            stats = crawler.get_stats()
            logger.info(f'''统计:
队列 = {stats['queue_size']},
已爬 = {stats['visited']},
保存 = {stats['saved']},
失败 = {stats['failed']},
重复={stats['duplicates']}''')
    stats_task = asyncio.create_task(print_stats())

    try:
        await asyncio.gather(*workers)
    except KeyboardInterrupt:
        logger.warning('\n！！！！！！用户停止爬虫！！！！！！')
    finally:
        crawler.export_results()
        crawler.export_errors()
        await crawler.close()
    stats = crawler.get_stats()
    logger.info(f'''统计:
队列 = {stats['queue_size']},
已爬 = {stats['visited']},
保存 = {stats['saved']},
失败 = {stats['failed']},
重复={stats['duplicates']}''')
    stats_task.cancel()

    logger.info("=" * 50)
    logger.info("爬虫程序结束")
    logger.info("=" * 50)

if __name__ == '__main__':
    asyncio.run(main())

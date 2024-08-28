import asyncio
import os
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Final, Optional

from requests.exceptions import ConnectionError as RequestsConnectionError, ConnectTimeout, ReadTimeout

import aiohttp
import requests

TIMEOUT: Final[int] = 5


def get_primes(number: int) -> list[int]:
    """1 задача. Получение списка простых чисел до number."""
    primes = []
    for i in range(2, number + 1):
        is_prime = True
        for j in range(2, int(i ** 0.5) + 1):
            if i % j == 0:
                is_prime = False
                break
        if is_prime:
            primes.append(i)
    return primes


class HTTPError(Exception):
    pass


class BaseContentSaver:
    def get_output_path(self, output_dir: str, url: str, folder_name: str = 'output') -> str:
        """
        Конечное название файла.

        :param output_dir: корневая директория для output файла
        :param url: URL
        :param folder_name: название папки для сохранения вебстраниц
        :return: str
        """
        if output_dir is None:
            output_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), folder_name)
        if not os.path.exists(output_dir):
            os.makedirs(output_dir, exist_ok=True)
        url_name = Path(url).name.replace('.', '_')
        return os.path.join(output_dir, url_name) + '.txt'


class WebContentSaverAsync(BaseContentSaver):
    """Класс сохранения контента сайтов ассинхронным подходом."""

    @staticmethod
    async def get_url_data(session: aiohttp.ClientSession, url: str) -> bytes:
        """Получение данных по URL."""
        try:
            async with session.get(url, timeout=TIMEOUT) as response:
                if response.status == 200:
                    return await response.read()
                else:
                    print(f'Something went wrong. URL={url}. Status={response.status}')
                    raise HTTPError
        except aiohttp.ClientConnectorError as err:
            print(f'ConnectionError. URL={url}')
            raise err
        except (aiohttp.ConnectionTimeoutError, asyncio.TimeoutError) as err:
            print(f'TimeoutError. URL={url}')
            raise err

    async def save_webpages_to_disk(
        self,
        session: aiohttp.ClientSession,
        url: str,
        output_dir: Optional[str] = None,
        folder_name: Optional[str]= None,
    ) -> None:
        """Сохранение вебстраниц на диск."""
        file_path = self.get_output_path(output_dir, url, folder_name)
        with open(file_path, 'wb') as file:
            try:
                file.write(await WebContentSaverAsync.get_url_data(session, url))
            except (
                HTTPError,
                aiohttp.ConnectionTimeoutError,
                aiohttp.ClientConnectorError,
                asyncio.TimeoutError,
            ):
                print(f'URL={url}. File removed')
                os.remove(file_path)


class WebContentSaver(BaseContentSaver):
    """Класс сохранения контента сайтов синхронным подходом."""

    @staticmethod
    def get_url_data(url: str) -> bytes:
        """Получение данных по URL."""
        try:
            response = requests.get(url, timeout=TIMEOUT)
        except (ConnectTimeout, ReadTimeout) as err:
            print(f'TimeoutError. URL={url}')
            raise err
        except RequestsConnectionError as err:
            print(f'ConnectionError. URL={url}')
            raise err
        else:
            if response.status_code == 200:
                return response.content
            else:
                print(f'Something went wrong. URL={url}')
                raise HTTPError

    def save_webpages_to_disk(
        self,
        url: str,
        output_dir: Optional[str] = None,
        folder_name: Optional[str] = None,
    ) -> None:
        """Сохранение вебстраниц на диск."""
        file_path = self.get_output_path(output_dir, url, folder_name)
        with open(file_path, 'wb') as file:
            try:
                file.write(WebContentSaver.get_url_data(url))
            except (HTTPError, RequestsConnectionError, ConnectTimeout, ReadTimeout):
                print(f'URL={url}. File removed')
                os.remove(file_path)
    

if __name__ == '__main__':
    """
    Во 2 задача сделал 3 подхода:
        1. С использованием ассинхронности
        2. С использованием синхроного подхода
        3. С использованием потоков

    Чтобы показать разницу увеличения производительности в разных подходах
    (особенно, если учитывать факт что запрос может отвалится/не дойти итд.)

    Единственный момент, что с асинхронным подходом пришлось тянуть большую либу aiohttp,

    когда же в синхронном и поточном подходах используется requests.

    Если сравнивать наглядно выполнение:
     - Ассинхронный подход от 5.2с до 6с
     - Синхронный подход от 110с до 180с
     - Потоки от 10с до 15с
    """
    # Задача 1
    primes_list = get_primes(19)

    # Задача 2
    urls = ['http://www.gmw.cn', 'https://www.djangoproject.com/', 'https://github.com/aio-libs/aiohttp', 'https://www.python.org/', 'http://www.yahoo.com', 'https://dzen.ru/', 'http://www.wikipedia.org', 'http://www.qq.com', 'https://habr.com/ru/feed/', 'http://www.twitter.com', 'http://www.live.com', 'http://www.taobao.com', 'http://www.bing.com', 'http://www.weibo.com', 'http://www.sina.com.cn', 'https://mail.ru/', 'http://www.yahoo.co.jp', 'http://www.msn.com', 'http://www.vk.com', 'http://www.google.de', 'http://www.yandex.ru', 'http://www.hao123.com', 'http://www.google.co.uk', 'http://www.reddit.com', 'http://www.ebay.com', 'http://www.google.fr', 'https://fastapi.tiangolo.com/', 'http://www.tmall.com', 'http://www.google.com.br', 'http://www.360.cn', 'http://www.sohu.com', 'http://www.amazon.co.jp', 'http://www.pinterest.com', 'https://docs.sqlalchemy.org/en/20/', 'http://www.google.it', 'http://www.google.ru', 'http://www.microsoft.com', 'http://www.google.es', 'http://www.wordpress.com', 'https://rt.rbc.ru/', 'http://www.tumblr.com', 'http://www.paypal.com', 'http://www.blogspot.com', 'https://stackoverflow.com/', 'http://www.stackoverflow.com', 'http://www.aliexpress.com', 'http://www.naver.com', 'http://www.ok.ru', 'http://www.apple.com', 'http://www.github.com', 'http://www.chinadaily.com.cn', 'http://www.imdb.com', 'http://www.google.co.kr', 'http://www.fc2.com', 'http://www.jd.com', 'http://www.blogger.com', 'http://www.163.com', 'http://www.google.ca', 'https://regex101.com/', 'http://www.amazon.in', 'http://www.office.com', 'http://www.google.co.id', 'http://www.youku.com', 'http://www.rakuten.co.jp', 'http://www.craigslist.org', 'http://www.amazon.de', 'http://www.nicovideo.jp', 'http://www.google.pl', 'http://www.soso.com', 'http://www.bilibili.com', 'http://www.dropbox.com', 'http://www.xinhuanet.com', 'http://www.outbrain.com', 'http://www.pixnet.net', 'http://www.alibaba.com', 'http://www.alipay.com', 'http://www.booking.com', 'https://www.jetbrains.com/', 'http://www.google.com.au', 'http://www.popads.net', 'http://www.cntv.cn', 'http://www.zhihu.com', 'http://www.amazon.co.uk', 'http://www.diply.com', 'http://www.coccoc.com', 'http://www.cnn.com', 'http://www.bbc.co.uk', 'http://www.twitch.tv', 'http://www.wikia.com', 'http://www.google.co.th', 'http://www.go.com', 'http://www.google.com.ph', 'http://www.doubleclick.net', 'http://www.onet.pl', 'http://www.googleadservices.com', 'https://www.sqlalchemy.org/', 'http://www.googleweblight.com', 'http://3dnews.ru']

    async def run_async():
        now = time.time()
        web_content_saver = WebContentSaverAsync()
        async with aiohttp.ClientSession() as session:
            tasks = [
                asyncio.create_task(web_content_saver.save_webpages_to_disk(session, url, folder_name='output_async')) for url in urls
            ]
            await asyncio.gather(*tasks)
        print(f'Async finished={time.time() - now}')

    def run_sync():
        now = time.time()
        web_content_saver = WebContentSaver()
        for url in urls:
            web_content_saver.save_webpages_to_disk(url, folder_name='output_sync')
        print(f'Sync finished={time.time() - now}')

    def run_threaded():
        now = time.time()
        web_content_saver = WebContentSaver()
        with ThreadPoolExecutor() as executor:
            [executor.submit(web_content_saver.save_webpages_to_disk, url, folder_name='output_threaded') for url in urls]
        print(f'Threaded finished={time.time() - now}')

    asyncio.run(run_async())
    run_sync()
    run_threaded()


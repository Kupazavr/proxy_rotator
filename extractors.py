from abc import ABC, abstractmethod
from typing import Union, List
import time
import requests
import logging
from multiprocessing.pool import ThreadPool
from helpers import chunkify
import gc
from user_agents import user_agents_list
import random
from storage import Proxy

format_message = u'%(filename)s[LINE:%(lineno)d]# %(levelname)-8s [%(asctime)s]  %(message)s'
logging.basicConfig(format=format_message, level=logging.DEBUG)
logging.getLogger("requests").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)
logging.getLogger("connectionpool").setLevel(logging.WARNING)


class ProxyExtractor(ABC):
    _logger = logging.getLogger('proxy_extractor')
    _pool_size = 100

    def __init__(self, check_url):
        self.check_url = check_url
        self._logger.info(f'{check_url=}')

    @abstractmethod
    def extract(self) -> List[str]:
        pass

    @staticmethod
    def _get_proxy_check_args(proxy=None) -> dict:
        user_agent_hardcore = 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko)' \
                              ' Chrome/84.0.4147.135 Safari/537.36'
        headers = {'cache-control': 'no-cache'}
        headers['user-agent'] = random.choice(user_agents_list) if user_agents_list else user_agent_hardcore
        proxies = {"http": proxy, "https": proxy}
        timeout = (10, 20)
        proxy_check_args = {'headers': headers,
                            'timeout': timeout,
                            'proxies': proxies}
        return proxy_check_args

    def _check_proxy(self, proxy_address):
        self._logger.debug(f"Checking {proxy_address} proxy started")
        try:
            proxy_check_args = self._get_proxy_check_args(proxy_address)
            request_check_time_limit = 30

            request_start_time = time.time()
            response = requests.get(self.check_url, stream=True, **proxy_check_args)
            for _ in response.iter_content(1024, decode_unicode=True):
                if time.time() - request_start_time > request_check_time_limit:
                    raise Exception
            request_time = time.time() - request_start_time
            self._logger.debug("Checking {} success with {} request time".format(proxy_address, request_time))

            return Proxy(address=proxy_address, prev_request_time=request_time)
        except Exception as e:
            self._logger.debug("Checking {} proxy failed because of | {}".format(proxy_address, e))
            return

    def transform(self, proxy_addresses: List[str]):
        self._logger.info('Start to transform proxies {} proxies'.format(len(proxy_addresses)))
        proxy_pool = ThreadPool(self._pool_size)
        transformed_proxies = list(filter(None, proxy_pool.map(self._check_proxy, proxy_addresses)))
        self._logger.info('Proxy transforming finished, left {}/{} proxies'.format(len(transformed_proxies),
                                                                                   len(proxy_addresses)))
        return transformed_proxies


class BestProxiesExtractor(ProxyExtractor):

    def __init__(self, key: str, check_url: str = 'https://www.google.com'):
        self._key = key
        self._check_url = check_url
        super().__init__(self._check_url)

    def extract(self) -> Union[List[str], type(None)]:
        self._logger.info('Start extract proxies from best-proxies')
        best_proxies_url = 'https://api.best-proxies.ru/proxylist.txt'
        params = {'key': self._key, 'type': 'http,https', 'limit': 0}
        timeout = (60, 60)

        response = requests.get(best_proxies_url, params=params, timeout=timeout)

        # returns txt format so need to split it up
        if response.status_code == 200:
            proxies_list = response.text.split('\r\n')
        else:
            proxies_list = None
            self._logger.error('Best proxies returns {} status_code'.format(response.status_code))
        return proxies_list


if __name__ == '__main__':
    a = Proxy(address='1', prev_request_time=16.1)

from storage import ProxyStorage, ProxySqliteStorage
from extractors import BestProxiesExtractor, ProxyExtractor, Proxy
import logging
import time


class ProxyRotator:
    def __init__(self, proxy_storage: ProxyStorage, extractor: ProxyExtractor):
        self._logger = logging.getLogger('proxy_rotator')
        self._proxy_storage = proxy_storage
        self._extractor = extractor

    @property
    def extractor(self) -> ProxyExtractor:
        return self._extractor

    @extractor.setter
    def extractor(self, extractor: ProxyExtractor) -> None:
        self._extractor = extractor

    def fill_storage(self) -> None:
        # python 3.8 required
        self._logger.debug('Fill storage with {} extractor'.format(str(self._extractor)))

        self._logger.info('Start to extract proxies')
        # extracting proxy addresses from choosed service
        extracted_proxies = self._extractor.extract()
        # check uptime and transform to Proxy object
        transformed_proxies = self._extractor.transform(extracted_proxies[:10])
        # add proxies to storage
        list(map(self._proxy_storage.insert_proxy, transformed_proxies))
        return

    def clear_storage(self) -> None:
        self._logger.info('Clear storage')
        self._proxy_storage.wipe()
        return

    def exception_decorator(self, func):
        def wrapper(*args, proxy, **kwargs):
            try:
                request_start_time = time.time()
                result = func(proxy, *args, **kwargs)
                request_time = time.time() - request_start_time

                self._proxy_storage.update_proxy(proxy, prev_request_time=request_time)
                return result
            except Exception as e:
                self.mark_proxy_as_failed(proxy)
                raise
            finally:
                self._proxy_storage.increment_field(proxy, 'threads_using', decrement=True)

        return wrapper

    def mark_proxy_as_failed(self, proxy: Proxy):
        self._logger.debug(f'marking proxy {proxy}')
        if (proxy.errors is not None) and proxy.errors + 1 > 5:
            self.delete_proxy(proxy)
        else:
            self._proxy_storage.increment_field(proxy, 'errors_count')
            self._proxy_storage.update_proxy(proxy, prev_error_time=time.time())

    def select_proxy(self, prev_error_time=60, threads_using=1) -> Proxy:
        # need to find best way to send filters
        filters = [('threads_using', '__gt__', threads_using),
                   ('prev_error_time', '__lt__', time.time() - prev_error_time)]
        order_by = ('threads_using', 'error_count', 'prev_request_time')
        proxy = self._proxy_storage.select_proxy(filters, order_by)
        if proxy:
            self._proxy_storage.increment_field(proxy, 'threads_using')
        return proxy

    def delete_proxy(self, proxy: Proxy):
        self._logger.debug('Delete proxy {}'.format(proxy))
        self._proxy_storage.delete_proxy(proxy)
        return


if __name__ == '__main__':
    key = ''
    extractor = BestProxiesExtractor(key, 'https://www.blockchain.com')
    storage = ProxySqliteStorage()
    rotator = ProxyRotator(storage, extractor)
    rotator.clear_storage()
    rotator.fill_storage()

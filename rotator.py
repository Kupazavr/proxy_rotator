from storage import ProxyStorage, ProxySqliteStorage
import os
from extractors import BestProxiesExtractor, ProxyExtractor
import logging


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

    def clear_storage(self):
        self._logger.info('Clear storage')
        self._proxy_storage.wipe()

if __name__ == '__main__':
    key =''
    extractor = BestProxiesExtractor(key, 'https://www.blockchain.com')
    storage = ProxySqliteStorage()
    rotator = ProxyRotator(storage, extractor)
    rotator.clear_storage()
    rotator.fill_storage()
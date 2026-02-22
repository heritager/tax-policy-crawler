# coding=utf-8

# 缓存工具类，用于判断一个页面是否已经被爬取过


# 根据url检查是否已经爬取过
from TaxPolicyCrawlerScrapy.util import ElasticSearchUtil

_url_cache = set()
_md5_cache = set()


def is_url_crawled(url):
    if not url:
        return False
    if url in _url_cache:
        return True
    if ElasticSearchUtil.exists_by_url(url):
        _url_cache.add(url)
        return True
    _url_cache.add(url)
    return False


# 根据md5检查是否已经爬取过
def is_md5_crawled(md5):
    if not md5:
        return False
    if md5 in _md5_cache:
        return True
    if ElasticSearchUtil.exists_by_md5(md5):
        _md5_cache.add(md5)
        return True
    _md5_cache.add(md5)
    return False

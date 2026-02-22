# coding=utf-8
import json
import math
from urllib.parse import urlencode

import scrapy

from TaxPolicyCrawlerScrapy import settings
from TaxPolicyCrawlerScrapy.items import PolicyItem, PolicySource
from TaxPolicyCrawlerScrapy.util import CacheUtil, Constants


class TaxPolicyExplainCrawler(scrapy.Spider):
    # 框架使用的属性，用于分类存储
    policy_source = PolicySource()
    doc_type = Constants.DocTypeChinaTax.doc_type
    policy_source['source'] = Constants.DocTypeChinaTax.source_name
    policy_source['policyType'] = Constants.DocTypeChinaTax.policy_types['policy_explain']

    # spider的名称，与setting配置里的一致；必须要有name属性，否则scrapy不做识别
    name = "TaxPolicyExplainCrawler"

    api_url = 'https://www.chinatax.gov.cn/search5/search/s'
    headers = {
        'User-Agent': (
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
            '(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36'
        ),
        'Referer': 'https://fgk.chinatax.gov.cn/zcfgk/c100015/list_zcjd.html',
    }

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        spider = super().from_crawler(crawler, *args, **kwargs)
        spider.page_size = crawler.settings.getint('CHINATAX_PAGE_SIZE', settings.CHINATAX_PAGE_SIZE)
        spider.max_pages = crawler.settings.getint('CHINATAX_MAX_PAGES', settings.CHINATAX_MAX_PAGES)
        return spider

    async def start(self):
        for request in self.start_requests():
            yield request

    def start_requests(self):
        yield self._build_page_request(0)

    def _build_query(self, page_num):
        return {
            'siteCode': 'bm29000002',
            'searchWord': '',
            'type': 1,
            'xxgkResolveType': '文字',
            'pageNum': page_num,
            'pageSize': self.page_size,
            'cwrqStart': '',
            'cwrqEnd': '',
            'column': '政策解读',
            'likeDoc': 0,
            'wordPlace': 0,
            'videoreSolveType': '',
            'orderBy': 5,
            'indexCode': 1,
            'searchSiteName': 'GSFFK',
        }

    def _build_page_request(self, page_num):
        query = urlencode(self._build_query(page_num))
        url = '{}?{}'.format(self.api_url, query)
        return scrapy.Request(
            url=url,
            method='GET',
            headers=self.headers,
            callback=self.parse_search_page,
            meta={'page_num': page_num},
            dont_filter=True,
        )

    def parse_search_page(self, response):
        try:
            payload = json.loads(response.text)
        except Exception:
            self.logger.warning('JSON parse failed at %s', response.url)
            return

        result_all = payload.get('searchResultAll') or {}
        records = result_all.get('searchTotal') or []
        page_num = int(response.meta.get('page_num', 0))

        for data in records:
            item = self._build_policy_item(data)
            if not item:
                continue
            if CacheUtil.is_url_crawled(item.get('url')):
                continue
            yield item

        if page_num == 0:
            total = int(result_all.get('total') or 0)
            if total <= 0:
                return

            page_size = max(1, int(self.page_size))
            page_count = int(math.ceil(float(total) / float(page_size)))
            if self.max_pages > 0:
                page_count = min(page_count, self.max_pages)

            for index in range(1, page_count):
                yield self._build_page_request(index)

    def _build_policy_item(self, data):
        url = data.get('url')
        if not url:
            return None

        content = data.get('content') or data.get('shortContent') or ''
        if not content:
            return None

        gov_doc = data.get('govDoc') or {}
        return PolicyItem(
            title=(data.get('title') or '').strip(),
            subtitle=(gov_doc.get('docNum') or data.get('label') or '').strip(),
            date=(data.get('cwrq') or '').split(' ')[0],
            content=content.strip(),
            publisher=(data.get('pubName') or data.get('source') or '').strip(),
            url=url.strip(),
            doc_type=self.doc_type,
            source=self.policy_source.get('source'),
            policyType=self.policy_source.get('policyType'),
            taxLevel=self.policy_source.get('taxLevel'),
        )

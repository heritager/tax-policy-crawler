# coding=utf-8
import json
import logging

import TaxPolicyCrawlerScrapy.util.Constants as Constants
from TaxPolicyCrawlerScrapy import settings

try:
    from elasticsearch import Elasticsearch
except Exception:
    Elasticsearch = None

logger = logging.getLogger(__name__)
_es_client = None
_client_initialized = False
_warned_disabled = False


def _is_enabled():
    return bool(getattr(settings, 'USE_ELASTICSEARCH', False)) and Elasticsearch is not None


def _normalize_total(total):
    if isinstance(total, dict):
        return int(total.get('value', 0))
    if total is None:
        return 0
    return int(total)


def _get_client():
    global _es_client, _client_initialized, _warned_disabled
    if not _is_enabled():
        if not _warned_disabled:
            logger.info('Elasticsearch is disabled. Set USE_ELASTICSEARCH=1 to enable persistence.')
            _warned_disabled = True
        return None

    if _client_initialized:
        return _es_client

    _client_initialized = True
    try:
        if getattr(settings, 'ES_URL', ''):
            _es_client = Elasticsearch(settings.ES_URL)
        else:
            _es_client = Elasticsearch(
                hosts=[{
                    'host': settings.ES_HOST,
                    'port': settings.ES_PORT,
                    'scheme': settings.ES_SCHEME,
                }]
            )
        _es_client.info()
    except Exception as ex:
        logger.warning('Failed to initialize Elasticsearch client: %s', ex)
        _es_client = None
    return _es_client


# 使用key在content里搜索
def search_by_key(key):
    client = _get_client()
    if not client:
        return {}

    ret = client.search(index=Constants.es_index_name, query={"match": {"content": key}})
    print(str(ret))
    return ret


def _exists_by_term(field_name, value):
    client = _get_client()
    if not client:
        return False

    try:
        ret = client.search(
            index=Constants.es_index_name,
            query={"term": {field_name: value}},
            size=1,
        )
        hits = ret.get('hits', {})
        return _normalize_total(hits.get('total')) > 0
    except Exception as ex:
        logger.warning('Elasticsearch term query failed for %s=%s: %s', field_name, value, ex)
        return False


# 根据url搜索，精确查找
def exists_by_url(url):
    return _exists_by_term('url', url)


# 根据md5搜索，精确查找
def exists_by_md5(md5):
    return _exists_by_term('hash_md5', md5)


# 删除索引
def delete_index(index_name):
    client = _get_client()
    if not client:
        return False
    client.indices.delete(index=index_name, ignore_unavailable=True)
    print("delete index '" + index_name + "' succeed")
    return True


# 查询索引
def get_index(index_name):
    client = _get_client()
    if not client:
        return {}
    return client.indices.get(index=index_name)


# 创建索引（# TODO：暂时没有支持setting）
def create_index(index_name, mapping=None, setting=None):
    client = _get_client()
    if not client:
        return False

    kwargs = {'index': index_name}
    if mapping:
        kwargs['mappings'] = mapping
    if setting:
        kwargs['settings'] = setting
    client.indices.create(**kwargs)
    return True


# 查询索引是否存在
def exists_index(index_name):
    client = _get_client()
    if not client:
        return False
    return bool(client.indices.exists(index=index_name))


# 保存爬取的数据
def save(index, doc_type, body):
    del doc_type  # 新版 Elasticsearch 不再使用 doc_type
    client = _get_client()
    if not client:
        return None

    document = body
    if isinstance(body, str):
        try:
            document = json.loads(body)
        except Exception:
            document = {"content": body}

    return client.index(index=index, document=document)

# 测试搜索
# create_index(Constants.es_index_name, mapping=Constants.default_es_mapping)# es_mapping)
# search_by_key('2017')
# delete_index(Constants.es_index_name)
# ElasticSearchPipeline.check_elastic_indices()
# print(exists_by_url("../../n810341/n810760/c1152203/content.html"))

# create_index(Constants.es_index_name, {
#     "policy_explain": {
#         "properties": {
#             "content": {
#                 "type": "text",
#                 "fields": {
#                     "keyword": {
#                         "type": "keyword",
#                         "ignore_above": 256
#                     }
#                 }
#             },
#             "date": {
#                 "type": "text",
#                 "fields": {
#                     "keyword": {
#                         "type": "keyword",
#                         "ignore_above": 256
#                     }
#                 }
#             },
#             "hash_md5": {
#                 "type": "string",
#                 "index": "not_analyzed",
#                 "fields": {
#                     "keyword": {
#                         "type": "keyword",
#                         "ignore_above": 256
#                     }
#                 }
#             },
#             "policyType": {
#                 "type": "text",
#                 "fields": {
#                     "keyword": {
#                         "type": "keyword",
#                         "ignore_above": 256
#                     }
#                 }
#             },
#             "publisher": {
#                 "type": "text",
#                 "fields": {
#                     "keyword": {
#                         "type": "keyword",
#                         "ignore_above": 256
#                     }
#                 }
#             },
#             "source": {
#                 "type": "text",
#                 "fields": {
#                     "keyword": {
#                         "type": "keyword",
#                         "ignore_above": 256
#                     }
#                 }
#             },
#             "timestamp": {
#                 "type": "float"
#             },
#             "title": {
#                 "type": "text",
#                 "fields": {
#                     "keyword": {
#                         "type": "keyword",
#                         "ignore_above": 256
#                     }
#                 }
#             },
#             "url": {
#                 "type": "string",
#                 "index": "not_analyzed",
#                 "fields": {
#                     "keyword": {
#                         "type": "keyword",
#                         "ignore_above": 256
#                     }
#                 }
#             }
#         }
#     }
# })


# print(get_index(Constants.es_index_name))

# print(exists_index(Constants.es_index_name))

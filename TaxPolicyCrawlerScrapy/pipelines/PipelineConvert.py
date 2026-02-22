# -*- coding: utf-8 -*-

# 在pipelines保存前的公共数据转换
import hashlib
import time
from TaxPolicyCrawlerScrapy.items import PolicySource
from TaxPolicyCrawlerScrapy.util import Constants


def get_es_body(doc_type, policy_source, item, hash_md5, timestamp):
    _dict = dict(policy_source) if policy_source else {}
    _dict.update(dict(item))
    _dict['hash_md5'] = hash_md5
    _dict['timestamp'] = timestamp
    _dict['doc_type'] = doc_type
    return _dict


def convert_item(item, spider=None):
    if not item or (not item.get('title') and not item.get('content')):
        print('Get an EMPTY item:' + str(item))
        return None

    doc_type = None
    policy_source = None
    if spider is not None:
        doc_type = getattr(spider, 'doc_type', None)
        policy_source = getattr(spider, 'policy_source', None)

    if not doc_type:
        doc_type = item.get('doc_type') or Constants.default_doc_type

    if not policy_source:
        policy_source = PolicySource(
            source=item.get('source'),
            policyType=item.get('policyType'),
            taxLevel=item.get('taxLevel'),
        )

    # Md5，及排重
    hash_source = item.get('content') or item.get('title') or ''
    hash_md5 = hashlib.md5(hash_source.encode('utf-8')).hexdigest()

    # 插入前的时间戳等字段
    timestamp = time.time()

    # 拼装成最终存储结构
    return doc_type, get_es_body(doc_type, policy_source, item, hash_md5, timestamp)

# coding=utf-8

# 代理管理类，调用外部的proxy_pool服务，来管理代理

import re
import requests
import TaxPolicyCrawlerScrapy.settings as setting
from TaxPolicyCrawlerScrapy import settings

# 简单的 ip:port 的正则表达式
ip_reg_str = r"^([0-9]{1,3}\.){3}[0-9]{1,3}:[0-9]{1,6}$"
proxy_base_url = "http://" + settings.PROXY_HOST + ":" + settings.PROXY_PORT


def get_proxy():
    if not setting.USE_PROXY:
        return None

    try:
        ip_address = requests.get(proxy_base_url + "/get/", timeout=10).text
    except Exception as ex:
        print("proxy service unavailable: {}".format(ex))
        return None

    if not ip_address:
        return {}

    pattern = re.compile(ip_reg_str)
    match = pattern.match(ip_address)
    if not match:
        return {}

    proxy = match.group()
    print("use proxy:" + proxy)
    return {"http": "http://{}".format(proxy)}


def delete_proxy(proxy):
    host = proxy.replace('http://', '').replace('https://', '')
    print("delete proxy:" + host)
    try:
        requests.get(proxy_base_url + "/delete/?proxy={}".format(host), timeout=10)
    except Exception as ex:
        print("delete proxy failed: {}".format(ex))


# print(get_proxy())

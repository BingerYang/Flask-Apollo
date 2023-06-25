# -*- coding: utf-8 -*-
# @Time     : 2023/5/16 15:34
# @Author   : binger
import hashlib
import json
import os.path
import threading
import time
import logging
from typing import Optional

import requests

logger = logging.getLogger(__name__)


def init_ip():
    try:
        import socket
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
            s.connect(('8.8.8.8', 53))
            ip = s.getsockname()[0]
            return ip
    finally:
        return ""


class Apollo(object):
    CONFIGURATIONS = "configurations"
    NOTIFICATION_ID = "notificationId"
    NAMESPACE_NAME = "namespaceName"
    LOOP_INTERVAL = 3

    def __init__(self, config_url, app_id, cluster="default", secret=None, keep_hot_update=False, request_timeout=None,
                 ip=None, namespaces=("application",), change_func=None):
        self.config_url = config_url
        self.cluster = cluster
        self.app_id = app_id

        self._ip = ip or init_ip()
        self._secret = secret

        self._request_timeout = request_timeout
        self._change_func = change_func

        self._stopping = False
        self._hot_syncing = False
        self._namespace_cache = {}
        self._notification_ids = []
        if namespaces:
            self.add_notification_ids(namespaces)
        if keep_hot_update:
            self.start()

    def get_value(self, key, default_val=None, namespace='application'):
        n_data = self._namespace_cache.get(namespace) or {}
        conf = n_data.get("CONFIGURATIONS") or {}
        return conf.get(key, default_val)

    def add_notification_ids(self, namespaces, notification_id=-1):
        for namespace in namespaces:
            self._notification_ids.append({self.NAMESPACE_NAME: namespace, self.NOTIFICATION_ID: notification_id})

    @property
    def is_syncing(self):
        return self._hot_syncing

    def stop(self):
        self._stopping = True
        logger.info("Stopping sync...")

    def run_forever(self, interval: Optional[int] = None):
        interval = interval or self.LOOP_INTERVAL
        self._hot_syncing = True
        if not self._namespace_cache:
            self.sync_for_app(self._notification_ids)

        while not self._stopping:
            time.sleep(interval)
            try:
                self.sync_for_app()
            except Exception as e:
                logger.error(f"sync for app: {e.args}")
        logger.info("sync has stopped!")
        self._hot_syncing = False
        return

    def start(self, catch_signals=True, use_eventlet=False):
        if self._hot_syncing:
            logger.warning("sync has running!!!")
            return

        self._hot_syncing = True
        if use_eventlet:
            import eventlet
            eventlet.monkey_patch()
            eventlet.spawn(self.run_forever)
        else:
            if catch_signals:
                import signal
                signal.signal(signal.SIGINT, self.stop)
                signal.signal(signal.SIGTERM, self.stop)
                signal.signal(signal.SIGABRT, self.stop)
            t = threading.Thread(target=self.run_forever)
            t.start()

    @staticmethod
    def signature(timestamp, uri, secret):
        import hmac
        import base64
        string_to_sign = f"{timestamp}\n{uri}"
        hmac_code = hmac.new(secret.encode(), string_to_sign.encode(), hashlib.sha1).digest()
        return base64.b64encode(hmac_code).decode()

    def _headers(self, url):
        headers = {}
        if not self._secret:
            return

        uri = url[len(self.config_url):-1]
        time_unix_now = str(int(round(time.time() * 1000)))
        headers['Authorization'] = 'Apollo ' + self.app_id + ':' + self.signature(time_unix_now, uri, self._secret)
        headers['Timestamp'] = time_unix_now
        return headers

    def load_data_from_namespace(self, namespace="application"):
        # 拉取 namespace 下配置
        url = '{}/configs/{}/{}/{}?releaseKey={}&ip={}'.format(
            self.config_url, self.app_id, self.cluster, namespace, "", self._ip)

        try:
            res = requests.get(url, headers=self._headers(url), timeout=self._request_timeout)
            code = res.status_code
            if code == 200:
                data = res.json()
                configurations = data["configurations"]
                if os.path.splitext(namespace)[1] == ".json":
                    configurations = json.loads(data["configurations"]["content"])

                return {self.CONFIGURATIONS: configurations}
            else:
                return None
        except Exception as e:
            logger.error(e)
            return None

    def _sync_data_from_namespace(self, namespace, notification_id: int):
        src_n_data = self._namespace_cache.get(namespace) or {}
        src_notification_id = src_n_data.get(self.NOTIFICATION_ID)
        if src_notification_id != notification_id:
            logger.info("recv different version data: {}".format(notification_id))
            n_data = self.load_data_from_namespace(namespace)
            if not n_data:
                # TODO: 不存在是否删除
                return
            n_data[self.NOTIFICATION_ID] = notification_id
            self._namespace_cache[namespace] = n_data

            try:
                # 处理监听推送
                callable(self._change_func) and self._change_func(namespace=namespace,
                                                                  notification_id=self.NOTIFICATION_ID,
                                                                  configurations=n_data[self.CONFIGURATIONS],
                                                                  old_configurations=src_n_data.get(self.CONFIGURATIONS))
            except Exception as e:
                logger.warning(f"change notification {namespace}")

    def sync_for_app(self, default_notifications=None):
        notifications = []  # 通知信息
        for namespace, n_data in self._namespace_cache.items():
            notifications.append({self.NAMESPACE_NAME: namespace, self.NOTIFICATION_ID: -1})
        if not notifications:
            notifications = default_notifications

        # 查看数据版本是否变化
        url = '{}/notifications/v2'.format(self.config_url)
        params = {
            'appId': self.app_id,
            'cluster': self.cluster,
            'notifications': json.dumps(notifications, ensure_ascii=False)
        }
        res = requests.get(url, params, timeout=self._request_timeout, headers=self._headers(url))
        http_code = res.status_code
        if http_code == 304:
            logger.debug("sync for app: no change！")
        elif http_code == 200:
            resp = res.json()
            logger.debug("sync for app: compare start...")
            for info in resp:
                self._sync_data_from_namespace(namespace=info[self.NAMESPACE_NAME],
                                               notification_id=info[self.NOTIFICATION_ID])
            logger.debug("sync for app: compare end!")
        else:
            logger.warning(f"sync for app: bnormal response http code:{http_code}, text: {res.text}")
        return True

    def sync_for_app_use_now_notifications(self, default_notifications=None):
        notifications = []  # 通知信息
        for namespace, n_data in self._namespace_cache.items():
            notification_id = n_data.get(self.NOTIFICATION_ID, -1)
            notifications.append({self.NAMESPACE_NAME: namespace, self.NOTIFICATION_ID: notification_id})
        if not notifications:
            notifications = default_notifications

        # 查看数据版本是否变化
        url = '{}/notifications/v2'.format(self.config_url)
        params = {
            'appId': self.app_id,
            'cluster': self.cluster,
            'notifications': json.dumps(notifications, ensure_ascii=False)
        }

        # 第一次请求小于 0.02, 后续请求会大约60s
        res = requests.get(url, params, headers=self._headers(url))
        http_code = res.status_code

        if http_code == 304:
            logger.debug("sync for app: no change！")
        elif http_code == 200:
            resp = res.json()
            logger.debug("sync for app: start ...")
            for info in resp:
                self._sync_data_from_namespace(namespace=info[self.NAMESPACE_NAME],
                                               notification_id=info[self.NOTIFICATION_ID])
            logger.debug("sync for app: end!")
        else:
            logger.warning(f"sync for app: bnormal response http code:{http_code}, text: {res.text}")
        return True


if __name__ == "__main__":
    apollo_config_url = "url"

    client = Apollo(app_id="common-config", cluster="default", config_url=apollo_config_url,
                    keep_hot_update=True)
    # val = client.get_value("CHAT_CHATGPT_H5_TIMEOUT", default_val="defaultVal", namespace="CountryCodeBlacklist")
    while client.is_syncing:
        time.sleep(2)
        print(client._namespace_cache)
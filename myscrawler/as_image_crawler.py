"""
as_image_crawler
"""

import threading
import yaml
import random
import time
import requests

from pathlib import Path

from .helper.persister import Persister

# define sql
ddl = "create table pages (url primary key asc, ts, status, desc)"
sql_check = "select 1 from pages order by rowid asc limit 1"
sql_update = "update pages set status = ? where url = ?"
sql_fetch_one_by_order = "select url, desc from pages where status = ? order by ts asc"
sql_check_before_insert = "select 1 from pages where url = ?"
sql_insert = (
    "insert into pages (url, ts, status, desc) values (?, current_timestamp, ?, ?)"
)

# define global variables
_proxy = None
_headers = {}
_internal = None
_baseurl = ""
_timeout = 180


def sleep(min, max):
    time.sleep(random.randint(min, min) / 1000)


class PageCollector(threading.Thread):
    def __init__(self, persister, entrypoint):
        self.persister = persister
        self.entrypoint = entrypoint

    def run(self):
        entrypoint = self.entrypoint
        while True:
            url = _baseurl + entrypoint
            res = requests.get(url, proxies=_proxy, headers=_headers)
            if _internal is not None:
                sleep(_internal["min"], _internal["max"])


class ImageSaver(threading.Thread):
    def __init__(self, persister):
        self.persister = persister

    def run(self):
        pass


class AsImageCrawler:
    def __init__(self, config_path, baseurl, entrypoint):
        global _baseurl
        baseurl = baseurl
        self.entrypoint = entrypoint
        with open(config_path) as file:
            # load config
            self.config = yaml.loads(file)
            # initail persister
            persistence = self.config["persistence"]
            sqlite = persistence["sqlite"]
            self.persister = Persister(file=sqlite["file"], check=sql_check, ddl=ddl)
            # setup proxy if exists
            if "proxy" in self.config:
                proxy = self.config["proxy"]
                p_type = proxy["type"]
                p_host = proxy["host"]
                global _proxy
                _proxy = {p_type: f"{p_type}://{p_host}"}
            # setup simulation
            if "simulation" in self.config:
                simulation = self.config["simulation"]
                if "user_agent" in simulation:
                    global _headers
                    _headers["user-agent"] = simulation["user_agent"]
                if "internal" in simulation:
                    global _internal
                    _internal = simulation["internal"]

    def start(self):
        # pages collector thread
        collector = PageCollector(persister=self.persister, entrypoint=self.entrypoint)
        # image saver thread
        saver = ImageSaver(persister=self.persister)
        # start threads
        collector.start()
        saver.start()

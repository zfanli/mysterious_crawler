"""
as_image_crawler
"""

import threading
import yaml
import random
import time
import requests
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-4s %(name)s::%(message)s",
)

from bs4 import BeautifulSoup
from pathlib import Path

from .helper import io_helper
from .helper.persister import Persister

# define sql
DDL = "create table pages (url primary key asc, ts, status, desc)"
SQL_CHECK = "select 1 from pages order by rowid asc limit 1"
SQL_UPDATE = "update pages set status = ? where url = ?"
SQL_FETCH_ONE_BY_ORDER = "select url, desc from pages where status = ? order by ts asc"
SQL_CHECK_BEFORE_INSERT = "select 1 from pages where url = ?"
SQL_INSERT = (
    "insert into pages (url, ts, status, desc) values (?, current_timestamp, ?, ?)"
)

# define status
STATUS_READY = "READY"
STATUS_FINISHED = "FINISHED"


# define global variables
_proxies = None
_headers = {}
_internal = None
_baseurl = ""
_timeout = 180
_output = "./temp"
_pages_threshold = 10
_images_threshold = 3


def sleep(min, max):
    time.sleep(random.randint(min, min) / 1000)


def get(url):
    return requests.get(url, proxies=_proxies, headers=_headers, timeout=_timeout)


class PageCollector(threading.Thread):
    def __init__(self, config, entrypoint):
        super().__init__()
        self.entrypoint = entrypoint
        self.config = config
        self.logger = logging.getLogger("PageCollector")

    def run(self):
        persister = Persister(**self.config)
        entrypoint = self.entrypoint
        # count for exit due to irrelevant recommendation appears
        count = 1
        while True:
            # perform http request
            try:
                url = _baseurl + entrypoint
                self.logger.info(f"Reached to new page {url}")
                res = get(url)

                if res.status_code != requests.codes.ok:
                    # try again latter
                    self.logger.warn(
                        f"Status code is not ok for {url}, {res.status_code}"
                    )
                    time.sleep(5)
                    persister.close()
                    return self.run()
            except:
                # try again latter
                self.logger.error(f"Failed to reach to the page {url}")
                time.sleep(5)
                persister.close()
                return self.run()

            soup = BeautifulSoup(res.text, "html.parser")
            # get page info from link
            links = soup.select(".recommentBox")
            pages = [{"url": x.a["href"], "desc": x.a["title"]} for x in links]

            # save page info
            for page in pages:
                existed = persister.fetchone(SQL_CHECK_BEFORE_INSERT, (page["url"],))
                if existed is None:
                    self.logger.info(f'save new url {page["url"]}, {page["desc"]}')
                    persister.execute(
                        SQL_INSERT, (page["url"], STATUS_READY, page["desc"])
                    )

            # setup next entrypoint
            entrypoint = pages[0]["url"]

            # Finish this page and sleep for a few seconds before go on
            if _internal is not None:
                # sleep a little bit more than saving images
                sleep(_internal["min"] * 10, _internal["max"] * 10)
                if count > _pages_threshold:
                    # restart crawlering
                    self.logger.info(f"Restart crawlering from initial entrypoint.")
                    return self.run()
                count += 1


class ImageSaver(threading.Thread):
    def __init__(self, config):
        super().__init__()
        self.config = config
        self.logger = logging.getLogger("ImageSaver")

    def run(self):
        persister = Persister(**self.config)
        while True:
            # get url from sqlite
            page = persister.fetchone(SQL_FETCH_ONE_BY_ORDER, (STATUS_READY,))

            # try again latter if no record fetched
            if page is None:
                time.sleep(5)
                persister.close()
                return self.run()

            page_url, desc = page

            try:
                url = _baseurl + page_url
                res = get(url)
            except:
                # try again latter
                self.logger.error(f"Failed to reach to the page {url}")
                time.sleep(5)
                persister.close()
                return self.run()

            soup = BeautifulSoup(res.text, "html.parser")
            # create output directory
            output = Path(_output).joinpath(desc)
            io_helper.ensure_path(output)
            # get start index by count files already in the dir
            start_idx = io_helper.count_files(output)

            # get image urls
            images = soup.select(".rootContant .showMiniImage")
            self.logger.info(f"Start dealing with page {page_url}, {desc}")

            for idx in range(start_idx, len(images)):
                image = images[idx]
                src = image["data-src"]
                # retrieve full size image uri
                src = src.replace("_t.", ".")
                filename = output.joinpath(str(idx) + "." + src.split(".")[-1])

                # retry saving image only
                for times in range(_images_threshold):
                    try:
                        url = _baseurl + src
                        res = get(url)
                        if res.status_code == requests.codes.ok:
                            self.logger.info(f"Saving image to {filename}")
                            io_helper.save_file(filename, res.content)
                            # break loop if ok
                            break
                        else:
                            # retry to threshold times and skip it if cannot reach
                            if times == _images_threshold - 1:
                                filename = str(filename) + "_" + str(res.status_code)
                                self.logger.warn(
                                    f"Retry times exceeded, skip and saving to {filename}"
                                )
                                io_helper.save_file(filename, b"")
                                break
                            # try again latter
                            self.logger.warn(
                                f"Status code is not ok for {url}, {res.status_code}"
                            )

                            time.sleep(5)
                            # try to save again
                            continue
                    except Exception as e:
                        self.logger.error(f"Failed to get the image {url}")
                        persister.close()
                        time.sleep(5)
                        return self.run()
                        # raise e

            # update stauts
            self.logger.info(f"Finished dealing with page {page_url}, {desc}")
            persister.execute(SQL_UPDATE, (STATUS_FINISHED, page_url))

            # Finish this page and sleep for a few seconds before go on
            if _internal is not None:
                sleep(_internal["min"], _internal["max"])


class AsImageCrawler:
    def __init__(self, config_path, baseurl, entrypoint, mute_collector=False):
        global _baseurl
        _baseurl = baseurl
        self.entrypoint = entrypoint
        self.logger = logging.getLogger("AsImageCrawler")
        # for mute collector if you don't need more collections
        self.mute_collector = mute_collector
        with open(config_path) as file:
            # load config
            self.config = yaml.load(file, Loader=yaml.Loader)

            # initail persister
            persistence = self.config["persistence"]
            sqlite = persistence["sqlite"]
            self.per_config = {"file": sqlite["file"], "check": SQL_CHECK, "ddl": DDL}

            # setup output path
            global _output
            _output = persistence["output"]
            # ensure output path exists
            io_helper.ensure_path(_output)

            # setup proxy if exists
            if "proxy" in self.config:
                proxy = self.config["proxy"]
                p_type = proxy["type"]
                p_host = proxy["host"]
                global _proxies
                _proxies = {p_type: f"{p_type}://{p_host}"}

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
        self.logger.info("Task running.")
        # collector is more efficient than image saver,
        # so you can mute collector if you don't need more collections
        if not self.mute_collector:
            # pages collector thread
            collector = PageCollector(
                config=self.per_config, entrypoint=self.entrypoint
            )
            collector.start()
        else:
            self.logger.info("Collector is muted.")
        # image saver thread
        saver = ImageSaver(config=self.per_config)
        saver.start()

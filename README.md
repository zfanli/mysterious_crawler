# mysterious_crawler

A set of crawlers for gathering some kinds of mysterious info.

## Install dependencies

```console
$ python3 -m pip install -r requirements.txt
```

Or using cdn.

```console
$ python3 -m pip install -r requirements.txt -i https://pypi.tuna.tsinghua.edu.cn/simple
```

## Usage

**as_image_crawler**

Saving images from ~~[masked]~~.

```python
from myscrawler.as_image_crawler import AsImageCrawler

baseurl = "https://example.com/"
entrypoint = "this_is_the_entrypoint"
aic = AsImageCrawler("./config/as_images/config.yml", baseurl, entrypoint)
aic.start()
```

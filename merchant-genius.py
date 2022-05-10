#!/usr/bin/env python
# -*- encoding: utf-8 -*-


"""Asynchronously get links embedded in multiple pages' HMTL."""
from bs4 import BeautifulSoup

import asyncio
import logging
import re
import sys
from typing import IO
import urllib.error
import urllib.parse
from fws import *

import aiofiles
import aiohttp
from aiohttp import ClientSession
from playwright_util import *
logging.basicConfig(
    format="%(asctime)s %(levelname)s:%(name)s: %(message)s",
    level=logging.DEBUG,
    datefmt="%H:%M:%S",
    stream=sys.stderr,
)
import pathlib
import sys

from usp.objects.page import (
    SitemapPage,
    SitemapNewsStory,
    SitemapPageChangeFrequency,
)
from usp.objects.sitemap import (
    IndexRobotsTxtSitemap,
    PagesXMLSitemap,
    IndexXMLSitemap,
    InvalidSitemap,
    PagesTextSitemap,
    IndexWebsiteSitemap,
    PagesRSSSitemap,
    PagesAtomSitemap,
)
from usp.tree import sitemap_tree_for_homepage

assert sys.version_info >= (3, 7), "Script requires Python 3.7+."
here = pathlib.Path(__file__).parent
logger = logging.getLogger("areq")
logging.getLogger("chardet.charsetprober").disabled = True

HREF_RE = re.compile(r'href="(.*?)"')

async def fetch_html(url: str, session: ClientSession, **kwargs) -> str:
    """GET request wrapper to fetch page HTML.

    kwargs are passed to `session.request()`.
    """

    resp = await session.request(method="GET", url=url, **kwargs)
    resp.raise_for_status()
    logger.info("Got response [%s] for URL: %s", resp.status, url)
    html = await resp.text()
    return html

async def parse(url: str, session: ClientSession, **kwargs) -> set:
    """Find HREFs in the HTML of `url`."""
    found = set()
    try:
        html = await fetch_html(url=url, session=session, **kwargs)

    except (
        aiohttp.ClientError,
        aiohttp.http_exceptions.HttpProcessingError,
    ) as e:
        logger.error(
            "aiohttp exception for %s [%s]: %s",
            url,
            getattr(e, "status", None),
            getattr(e, "message", None),
        )
        return found
    except Exception as e:
        logger.exception(
            "Non-aiohttp exception occured:  %s", getattr(e, "__dict__", {})
        )
        return found
    else:
        # print(html)
        text = BeautifulSoup(html, "html.parser")
        # print('raw html',text)
        hrefs = {i.get("href") for i in text.find_all(
            href=True)}
        print('count href link',len(hrefs))

        # Loop over the URLs in the current page
        for anchor in hrefs:
        # for link in text.find_all('a'):
            # extract link url from the anchor
            # anchor = link.attrs["href"] if "href" in link.attrs else ''
            # print('---',anchor)
            if any(anchor.startswith(i) for i in ["mailto:", "data:image","tel:", "javascript:", "#content-middle", "about:blank", "skype:"]):
                continue
            if any(anchor.endswith(i) for i in [".js", ".css",".png", ".webp", ".jpg", ".jpeg", ".txt",".json",".svg"]):
                continue                
            if anchor == "#" or "linkedin" in anchor or "\\" in anchor:
                continue
            if anchor.startswith('//'):
                local_link = anchor
                found.add(local_link)                
            elif anchor.startswith('/shop/url/'):
                local_link = anchor.replace('/shop/url/','')

                found.add(local_link)

        # logger.info("Found %d links for %s", len(found), url)
        print(found)

        return found
async def write_one(file: IO, url: str, **kwargs) -> None:
    """Write the found HREFs from `url` to `file`."""
    res = await parse(url=url, **kwargs)
    if not res:
        return None
    async with aiofiles.open(file, "a") as f:
        for p in res:
            print('p',p)
            if 'https://www.merchantgenius.io' in url:
                url=url.replace('https://www.merchantgenius.io','')
            await f.write(f"{url}\t{p}\n")
        logger.info("Wrote results for source URL: %s", url)

async def bulk_crawl_and_write(file: IO, urls: set, **kwargs) -> None:
    """Crawl & write concurrently to `file` for multiple `urls`."""
    async with ClientSession() as session:
        tasks = []
        for url in urls:
            tasks.append(
                write_one(file=file, url=url, session=session, **kwargs)
            )
        await asyncio.gather(*tasks)


async def getcatolinks():
  pl=pl_util()
  page=await pl.startpage('https://www.merchantgenius.io')
  links=[]
  yuefen = page.locator('xpath=//html/body/main/div/div[2]/a')
  for i in range(await yuefen.count()):
    suburl = await yuefen.nth(i).getAttribute('href')
    if suburl:
      url = 'https://www.merchantgenius.io' + suburl
      links.append(url)
  return links

    # urls=asyncio.run(getcatolinks())
def suburls():
    urls=['https://www.merchantgenius.io']
    outpath = here.joinpath("found_suburls.txt")
    with open(outpath, "w") as outfile:
        outfile.write("source_url\tparsed_url\n")

    asyncio.run(bulk_crawl_and_write(file=outpath, urls=urls))


def list_split(items, n):
    return [items[i:i+n] for i in range(0, len(items), n)]

    # return found_suburls
def shopurls():
    found_suburls=[]
    with open(here.joinpath("found_suburls.txt")) as infile:
        found_suburls = set(map(str.strip, infile))    
    outpath1 = here.joinpath("shop_urls.txt")
    found_suburls=[x.split('\t')[-1] for x in found_suburls]
    # print(found_suburls,'============')
    with open(outpath1, "w") as outfile1:
        outfile1.write("source_url\tparsed_url\n")
    t =list_split(found_suburls,10)
    for i in range(len(t)):
        # print(t[i])
        asyncio.run(bulk_crawl_and_write(file=outpath1, urls=t[i]))

async def write_one_locs(file: IO, url: str, **kwargs) -> None:

    print('==============usp is started==============')

    # start = time.time()


    tree = sitemap_tree_for_homepage(url)
    # SitemapPage(url=https://www.indiehackers.com/forum/the-business-of-podcasting-with-jeff-meyerson-of-software-engineering-daily-e2b157d5de, priority=0.2, last_modified=2019-09-04 18:27:13+00:00, change_frequency=SitemapPageChangeFrequency.MONTHLY, news_story=None)
    if InvalidSitemap in tree.sub_sitemaps:
        print('you need last straw')
        urls = crawler(url, 1)

    else:
        robot=tree.sub_sitemaps[0].url
        indexxmlsimap=[ x.url for x in tree.sub_sitemaps[0].sub_sitemaps]
        urls=[ x.url for x in tree.all_pages]
    if not urls:
        return None
    async with aiofiles.open(file, "a") as f:
        for p in urls:
            print('p',p)
            if 'https://www.merchantgenius.io' in url:
                url=url.replace('https://www.merchantgenius.io','')
            await f.write(f"{url}\t{p}\n")
        logger.info("Wrote results for source URL: %s", url)

async def bulk_crawl_and_write_loc(file: IO, urls: set, **kwargs) -> None:
    """Crawl & write concurrently to `file` for multiple `urls`."""
    async with ClientSession() as session:
        tasks = []
        for url in urls:
            url='https://'+url
            tasks.append(
                write_one_locs(file=file, url=url, session=session, **kwargs)
            )
        await asyncio.gather(*tasks)

def usp():
    found_suburls=[]
    with open(here.joinpath("shop_urls.txt")) as infile:
        found_suburls = set(map(str.strip, infile))    
    outpath1 = here.joinpath("shop_urls_locs.txt")
    found_suburls=[x.split('\t')[-1] for x in found_suburls]
    # print(found_suburls,'============')
    with open(outpath1, "w") as outfile1:
        outfile1.write("source_url\tparsed_url\n")
    t =list_split(found_suburls,5)
    for i in range(len(t)):
        # print(t[i])
        asyncio.run(bulk_crawl_and_write_loc(file=outpath1, urls=t[i]))

if __name__ == "__main__":
    # found_suburls=suburls()
    # shopurls()
    usp()

import requests
import sys
from pathlib import Path
import json
import threading

from bs4 import BeautifulSoup
from utils.http_client import HttpClient, HttpClientConfig

FILE = Path(__file__).resolve()
ROOT = FILE.parents[1]  # root directory
if str(ROOT) not in sys.path:
    sys.path.append(str(ROOT))  # add ROOT to PATH

from logger import log
from crawler.base_crawler import BaseCrawler
from utils.bs4_utils import get_text_from_tag

# module-level lock for safe concurrent appends
_write_lock = threading.Lock()


class VietNamNetCrawler(BaseCrawler):

    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)
        self.logger = log.get_logger(name=__name__)
        self.base_url = "https://vietnamnet.vn"
        # polite HTTP client
        self.http = HttpClient(
            logger=self.logger,
            config=HttpClientConfig(
                max_rps=getattr(self, "max_rps", 0.5),
                timeout=getattr(self, "timeout", 15.0),
                retry_total=getattr(self, "retry_total", 5),
                retry_backoff=getattr(self, "retry_backoff", 0.5),
                rotate_user_agent=True,
                respect_robots=getattr(self, "respect_robots", False),
                proxy=getattr(self, "proxy", None),
            ),
        )
        self.article_type_dict = {
            0: "thoi-su",
            1: "kinh-doanh",
            2: "the-thao",
            3: "van-hoa-giai-tri",
            4: "cong-nghe",
            5: "the-gioi",
            6: "doi-song",
            7: "giao-duc",
            8: "suc-khoe",
            9: "chinh-tri",
            10: "phap-luat",
            11: "oto-xe-may",
            12: "bat-dong-san",
            13: "du-lich",
            14: "dan-toc-ton-giao"
        }   
        
    def extract_content(self, url: str) -> tuple:
        """
        Extract title, sapo (description) and full content text from url
        Returns (title, sapo_text, content_text) all as strings or (None, None, None) on failure
        """
        content = self.http.get(url).content
        soup = BeautifulSoup(content, "html.parser")

        title_tag = soup.find("h1", class_="content-detail-title") 
        desc_tag = soup.find("h2", class_=["content-detail-sapo", "sm-sapo-mb-0"])
        p_tag = soup.find("div", class_=["maincontent", "main-content"])

        if [var for var in (title_tag, desc_tag, p_tag) if var is None]:
            return None, None, None
        
        title = title_tag.get_text(strip=True)

        # sapo_text: nối tất cả phần trong desc_tag
        desc_parts = [get_text_from_tag(p).strip() for p in desc_tag.contents if get_text_from_tag(p).strip()]
        sapo_text = " ".join(desc_parts)

        # paragraphs: nối tất cả <p> thành nội dung văn bản
        para_texts = [get_text_from_tag(p).strip() for p in p_tag.find_all("p") if get_text_from_tag(p).strip()]
        content_text = "\n".join([title] + para_texts) if para_texts else title

        return title, sapo_text, content_text

    def write_content(self, url: str, output_fpath: str) -> bool:
        """
        From url, extract title, sapo and full content then append a JSON record to a common file.
        Record format:
        {
            "instruction": "Tóm tắt văn bản sau",
            "input": content_text,
            "output": sapo_text
        }
        All records are appended to the same file: <output_dpath>/records.jsonl (fallback to current dir).
        """
        title, sapo_text, content_text = self.extract_content(url)
                    
        if title is None:
            return False

        record = {
            "instruction": "Tóm tắt văn bản sau",
            "input": content_text,
            "output": sapo_text
        }

        # determine common output file in output_dpath (fallback to cwd)
        out_dir = Path(getattr(self, "output_dpath", "."))
        out_dir.mkdir(parents=True, exist_ok=True)
        central_fpath = out_dir / "records.jsonl"

        try:
            with _write_lock:
                with open(central_fpath, "a", encoding="utf-8") as file:
                    file.write(json.dumps(record, ensure_ascii=False) + "\n")
            return True
        except Exception as e:
            self.logger.error(f"Failed to write record for {url}: {e}")
            return False
    
    def get_urls_of_type_thread(self, article_type, page_number):
        """" Get urls of articles in a specific type in a page"""
        page_url = f"https://vietnamnet.vn/{article_type}-page{page_number}"
        content = self.http.get(page_url).content
        soup = BeautifulSoup(content, "html.parser")
        titles = soup.find_all(class_=["horizontalPost__main-title", "vnn-title", "title-bold"])

        if (len(titles) == 0):
            self.logger.info(f"Couldn't find any news in {page_url} \nMaybe you sent too many requests, try using less workers")
            
        articles_urls = list()

        for title in titles:
            full_url = title.find_all("a")[0].get("href")
            if self.base_url not in full_url:
                full_url = self.base_url + full_url
            articles_urls.append(full_url)
    
        return articles_urls

import requests
import sys
from pathlib import Path
import json
import re
import threading

from bs4 import BeautifulSoup


FILE = Path(__file__).resolve()
ROOT = FILE.parents[1]  # root directory
if str(ROOT) not in sys.path:
    sys.path.append(str(ROOT))  # add ROOT to PATH

from utils.http_client import HttpClient, HttpClientConfig
from logger import log
from crawler.base_crawler import BaseCrawler
from utils.bs4_utils import get_text_from_tag

# module-level lock for safe concurrent appends
_write_lock = threading.Lock()

class DanTriCrawler(BaseCrawler):

    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)
        self.logger = log.get_logger(name=__name__)
        self.base_url = "https://dantri.com.vn"
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
                pool_connections=getattr(self, "pool_connections", 50),
                pool_maxsize=getattr(self, "pool_maxsize", 100),
            ),
        )
        self.article_type_dict = {
            0: "doi-song",
            1: "the-gioi",
            2: "kinh-doanh",
            3: "bat-dong-san",
            4: "the-thao",
            5: "noi-vu",
            6: "tam-long-nhan-ai",
            7: "suc-khoe",
            8: "cong-nghe",
            9: "giai-tri",
            10: "thoi-su",
            11: "giao-duc",
            12: "du-lich"
        }
        # Mapping for display category names
        self.category_display_dict = {
            "doi-song": "Đời sống",
            "the-gioi": "Thế giới",
            "kinh-doanh": "Kinh doanh",
            "bat-dong-san": "Bất động sản",
            "the-thao": "Thể thao",
            "noi-vu": "Nội vụ",
            "tam-long-nhan-ai": "Tấm lòng nhân ái",
            "suc-khoe": "Sức khỏe",
            "cong-nghe": "Công nghệ",
            "giai-tri": "Giải trí",
            "thoi-su": "Thời sự",
            "giao-duc": "Giáo dục",
            "du-lich": "Du lịch"
        }
        # Track current article type being crawled
        self.current_article_type = None

    def crawl_type(self, article_type, urls_dpath, results_dpath):
        """" Crawl total_pages of articles in specific type """
        self.logger.info(f"Crawl articles type {article_type}")
        # Set current article type for category extraction
        self.current_article_type = article_type
        error_urls = list()
        
        # getting urls
        self.logger.info(f"Getting urls of {article_type}...")
        articles_urls = self.get_urls_of_type(article_type)
        articles_urls_fpath = "/".join([urls_dpath, f"{article_type}.txt"])
        with open(articles_urls_fpath, "w") as urls_file:
            urls_file.write("\n".join(articles_urls)) 

        # crawling urls
        self.logger.info(f"Crawling from urls of {article_type}...")
        results_type_dpath = "/".join([results_dpath, article_type])
        error_urls = self.crawl_urls(articles_urls_fpath, results_type_dpath)
        
        return error_urls
        
    def extract_content(self, url: str) -> tuple:
        """
        Extract title, description, paragraphs and category from url
        @param url (str): url to crawl
        @return title (str)
        @return description (generator)
        @return paragraphs (generator)
        @return category (str)
        """
        content = self.http.get(url).content
        soup = BeautifulSoup(content, "html.parser")

        title = soup.find("h1", class_="title-page detail") 
        if title == None:
            return None, None, None, None
        title = title.text

        # Get category from current article type being crawled
        category = "Tin tức"  # default
        if self.current_article_type:
            category = self.category_display_dict.get(
                self.current_article_type, 
                self.current_article_type.replace("-", " ").title()
            )

        description_tag = soup.find("h2", class_="singular-sapo")
        description = (get_text_from_tag(p) for p in description_tag.contents) if description_tag else []
        
        content_div = soup.find("div", class_="singular-content")
        paragraphs = (get_text_from_tag(p) for p in content_div.find_all("p")) if content_div else []

        return title, description, paragraphs, category

    def write_content(self, url: str, output_fpath: str) -> bool:
        """
        From url, extract title, description and paragraphs then append JSON record
        to a single file: <output_dpath>/records.jsonl

        record = {
            "title": title,
            "input": content_text,
            "output": sapo_text,
            "category": category,
            "source": "Dantri"
        }
        """
        title, description, paragraphs, category = self.extract_content(url)
                    
        if title == None:
            return False

        # materialize generators to lists
        description_list = list(description) if description is not None else []
        paragraphs_list = list(paragraphs) if paragraphs is not None else []

        # sapo_text is the short description (sapo)
        sapo_text = "\n".join([p.strip() for p in description_list if p is not None])

        # remove leading source tag like "(Dân trí) - " or any "(...)" followed by dash/colon
        sapo_text = re.sub(r'^\([^)]*\)\s*[-–—:]\s*', '', sapo_text).strip()

        # content_text is the full article text; include title and paragraphs
        body_text = "\n".join([p.strip() for p in paragraphs_list if p is not None])
        content_text = title.strip() + ("\n" + body_text if body_text else "")

        record = {
            "title": title.strip(),
            "input": content_text,
            "output": sapo_text,
            "category": category,
            "source": "Dantri"
        }

        # ensure output directory exists and append JSON line to a single file
        out_dir = Path(getattr(self, "output_dpath", "."))  # fallback to current dir
        out_dir.mkdir(parents=True, exist_ok=True)
        # Unify output file name across crawlers
        central_fpath = out_dir / "dantri_records.jsonl"

        with _write_lock:
            with open(central_fpath, "a", encoding="utf-8") as file:
                file.write(json.dumps(record, ensure_ascii=False) + "\n")

        return True
    
    def get_urls_of_type_thread(self, article_type, page_number):
        """" Get urls of articles in a specific type in a page"""
        page_url = f"https://dantri.com.vn/{article_type}/trang-{page_number}.htm"
        content = self.http.get(page_url).content
        soup = BeautifulSoup(content, "html.parser")
        titles = soup.find_all(class_="article-title")

        if (len(titles) == 0):
            self.logger.info(f"Couldn't find any news in {page_url} \nMaybe you sent too many requests, try using less workers")
            

        articles_urls = list()

        for title in titles:
            link = title.find_all("a")[0]
            href = link.get("href")
            full_url = href
            # if href is relative like "/thoi-su/..." prepend base_url,
            # but if it's already absolute (starts with "http") keep it as is
            if not href.startswith("http"):
                # ensure we don't produce double slashes
                if href.startswith("/"):
                    full_url = self.base_url + href
                else:
                    full_url = self.base_url + "/" + href
            articles_urls.append(full_url)
    
        return articles_urls

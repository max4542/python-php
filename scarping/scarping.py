from supabase import create_client, Client
import requests
from bs4 import BeautifulSoup
import os
from datetime import datetime
import re
import random
from selenium import webdriver
from selenium.webdriver.chrome.options import Options

class SupabaseHandler:
    def __init__(self, url: str, key: str):
        self.supabase: Client = create_client(url, key)
        self.storage_bucket = "productimages"

    def insert_product(self, product_data: dict):
        """Insert product data into Supabase."""
        try:
            self.supabase.table("products").insert(product_data).execute()
            print(f"Inserted: {product_data['name']}")
        except Exception as e:
            print("Supabase insert error:", e)

    def upload_image(self, image_path: str, filename: str):
        """Upload image to Supabase storage."""
        try:
            files = self.supabase.storage.from_(self.storage_bucket).list()
            if any(f["name"] == filename for f in files):
                return filename

            with open(image_path, "rb") as f:
                self.supabase.storage.from_(self.storage_bucket).upload(
                    filename,
                    f,
                    {"content-type": "image/jpeg"}
                )
            return filename
        except Exception as e:
            print("Image upload error:", e)
            return None


class ImageHandler:
    def __init__(self, image_dir="scarp_images"):
        self.image_dir = image_dir
        os.makedirs(self.image_dir, exist_ok=True)

    def download_image(self, url, product_name, supabase: SupabaseHandler):
        """Download image from URL and save it locally."""
        if not url:
            return None

        try:
            res = requests.get(url, timeout=10)
            res.raise_for_status()
        except:
            return None

        filename = self._save(res.content, product_name)
        supabase.upload_image(os.path.join(self.image_dir, filename), filename)
        return filename

    def _save(self, content, name):
        """Save image content to a file and return the filename."""
        safe = re.sub(r'[\\/*?:"<>|]', "", name)
        filename = f"{safe}_{random.randint(1000,9999)}.jpg"
        path = os.path.join(self.image_dir, filename)
        with open(path, "wb") as f:
            f.write(content)
        return filename

class FlipkartScraper:

    BASE_URL = "https://www.flipkart.com/search?q={}"

    def __init__(self, products, supabase_handler):
        self.products = products
        self.supabase = supabase_handler
        self.image_handler = ImageHandler()
        self.driver = self._setup_driver()

    def _setup_driver(self):
        """Setup Selenium WebDriver in headless mode."""
        options = Options()
        options.add_argument("--headless")
        options.add_argument("--disable-gpu")
        return webdriver.Chrome(options=options)

    def scrape_all(self):
        """Scrape all products in the list."""
        for product in self.products:
            print(f"\nScraping: {product}")
            self.scrape_page(self.BASE_URL.format(product.replace(" ", "%20")))

    def scrape_page(self, url):
        """Scrape a single search results page."""
        self.driver.get(url)
        soup = BeautifulSoup(self.driver.page_source, "html.parser")

        product_links = soup.select("a[href*='/p/']")

        seen = set()

        for link in product_links:
            href = link.get("href")
            if not href or href in seen:
                continue
            seen.add(href)

            container = link.parent
            self.scrape_product(container, link)

    def scrape_product(self, container, link):
        """Scrape individual product details and insert into Supabase."""
        img = link.find("img")
        if not img:
            print("No image found, skipping product.")
            return

        name = img.get("alt")
        image_url = img.get("src") or img.get("data-src")

        price_text = container.find(string=re.compile("₹"))
        if not name or not image_url or not price_text:
            print("Missing data, skipping product.")
            return

        price = self._clean_price(price_text)

        image_filename = self.image_handler.download_image(
            image_url, name, self.supabase
        )

        self.supabase.insert_product({
            "name": name,
            "amount": price,
            "type": "Watch",
            "category": "Watch",
            "brand": "Nothing",
            "rating": round(random.uniform(3.5, 4.9), 1),
            "reviews_count": random.randint(50, 5000),
            "description": name,
            "banner_url": image_filename
        })

    def _clean_price(self, text):
        """Extract numeric price from text."""
        value = re.sub(r"[^\d]", "", text)
        return float(value) if value else random.randint(500, 1500)

if __name__ == "__main__":

    user_input = input("Enter product names (comma-separated): ")
    products = [p.strip() for p in user_input.split(",")]

    SUPABASE_URL = "https://fzliiwigydluhgbuvnmr.supabase.co/"
    SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImZ6bGlpd2lneWRsdWhnYnV2bm1yIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NDE5MjkxNTMsImV4cCI6MjA1NzUwNTE1M30.w3Y7W14lmnD-gu2U4dRjqIhy7JZpV9RUmv8-1ybQ92w"

    supabase_handler = SupabaseHandler(SUPABASE_URL, SUPABASE_KEY)
    scraper = FlipkartScraper(products, supabase_handler)
    scraper.scrape_all()

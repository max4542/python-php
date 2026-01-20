from supabase import create_client, Client
import requests
from bs4 import BeautifulSoup
import os
from datetime import datetime
import re
import random
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
import json


class SupabaseHandler:
    """Handles Supabase operations such as inserting products and uploading images."""

    def __init__(self, url: str, key: str):
        self.supabase: Client = create_client(url, key)
        self.storage_bucket = 'productimages'

    def insert_product(self, product_data: dict):
        """Inserts product data into the Supabase 'products' table."""
        try:
            self.supabase.table("products").insert(product_data).execute()
            print(f"Inserted into Supabase: {product_data.get('name')}")
        except Exception as e:
            print(f"Failed to insert {product_data.get('name')} into Supabase:", e)

    def upload_image(self, image_path: str, filename: str):
        """Uploads image to Supabase storage if not exists already."""
        try:
            existing_files = self.supabase.storage.from_(self.storage_bucket).list()
            if any(f['name'] == filename for f in existing_files):
                print(f"Image already exists: {filename}, skipping upload.")
                return filename

            with open(image_path, 'rb') as f:
                self.supabase.storage.from_(self.storage_bucket).upload(
                    path=filename,
                    file=f,
                    file_options={"content-type": "image/jpeg"}
                )
            return filename
        except Exception as e:
            print(f"Error uploading {filename} to Supabase:", e)
            return None


class ImageHandler:
    """Handles downloading and saving product images."""

    def __init__(self, image_dir: str = 'scarp_images'):
        self.image_dir = image_dir
        os.makedirs(self.image_dir, exist_ok=True)

    def download_image(self, image_url: str, product_name: str, supabase: SupabaseHandler):
        """Downloads an image from a URL and saves it locally."""
        if not image_url:
            print(f"No image URL for {product_name}")
            return None

        try:
            response = requests.get(image_url)
            response.raise_for_status()
        except requests.RequestException:
            print(f"Failed to download image for {product_name}")
            return None

        filename = self.save_image_locally(response.content, product_name)
        # uploaded_filename = supabase.upload_image(os.path.join(self.image_dir, filename), filename)
        return filename

    def save_image_locally(self, image_content: bytes, product_name: str):
        """Saves image content to a local file with a unique filename."""
        safe_name = re.sub(r'[\\/*?:"<>|]', "", product_name)
        filename = f"{safe_name}_{datetime.now().strftime('%Y%m%d')}_{random.randint(1000,9999)}.jpg"
        path = os.path.join(self.image_dir, filename)
        with open(path, 'wb') as f:
            f.write(image_content)
        return filename


class FlipkartScraper:
    """Scrapes product data from Flipkart and inserts it into Supabase."""

    BASE_URL = "https://www.flipkart.com/search?q={}&otracker=search"

    PRODUCT_CLASSES = ["tUxRFH", "IRpwTa", "s1Q9rs", "_1sdMkc LFEi7Z","_4WELSP", "jIjQ8S", "MZeksS"]

    PRICE_CLASSES = ["Nx9bqj _4b5DiR", "rgWa7D"]

    LINK_CLASSES = ["CGtC98", "IRpwTa", "s1Q9rs"]

    def __init__(self, product_names: list, supabase_handler: SupabaseHandler):
        self.product_names = product_names
        self.supabase = supabase_handler
        self.image_handler = ImageHandler()
        self.driver = self._setup_selenium()
        self.data = []
        self.total = []

    def _setup_selenium(self):
        options = Options()
        options.add_argument("--headless")
        return webdriver.Chrome(options=options)

    def scrape_all_products(self):
        for product in self.product_names:
            print(f"Scraping: {product}")
            self.total.extend(self.scrape_product_page(self.BASE_URL.format(product.replace(" ", "%20"))))
            print(f"Finished scraping: {product}\n")
        print(json.dumps(self.total, indent=4))

    def scrape_product_page(self, url: str):
        self.driver.get(url)
        soup = BeautifulSoup(self.driver.page_source, 'html.parser')
        containers = soup.find_all('div', class_=self.PRODUCT_CLASSES)

        if not containers:
            print(f"No products found for URL: {url}")
            return

        for container in containers:
            self.scrape_product(container)

        return self.data

    def scrape_product(self, container):
        image_tag = container.find('img')
        image_url = image_tag.get('src') or image_tag.get('data-src') if image_tag else None
        product_name = image_tag.get('alt', 'Unnamed Product') if image_tag else 'Unnamed Product'
        price = self._extract_price(container)
        link = self._extract_link(container)
        image_filename = self.image_handler.download_image(image_url, product_name, self.supabase)

        return self.data.append({
            "name": product_name,
            "amount": self._clean_price(price),
            "type": "Watch",
            "category": "Watch",
            "brand": "Google",
            "rating": round(random.uniform(3.0, 5.0), 1),
            "reviews_count": random.randint(50, 5000),
            "description": product_name,
            "banner_url": image_filename,
            "product_link": link
        })

        # self.supabase.insert_product(product_data)

    def _extract_price(self, container):
        price_tag = container.find('div', class_=self.PRICE_CLASSES)
        return price_tag.text.strip() if price_tag else None

    def _extract_link(self, container):
        link_tag = container.find('a', class_=self.LINK_CLASSES)
        if link_tag and 'href' in link_tag.attrs:
            return "https://www.flipkart.com" + link_tag['href']
        return None

    def _clean_price(self, price_str):
        if not price_str or not isinstance(price_str, str):
            return random.randint(300, 1000)
        clean = re.sub(r'[^\d.]', '', price_str)
        return float(clean) if clean else random.randint(300, 1000)


if __name__ == "__main__":
    user_input = input("Enter product names (comma-separated): ")
    products = [p.strip() for p in user_input.split(",")]

    SUPABASE_URL = "https://fzliiwigydluhgbuvnmr.supabase.co"
    SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImZ6bGlpd2lneWRsdWhnYnV2bm1yIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NDE5MjkxNTMsImV4cCI6MjA1NzUwNTE1M30.w3Y7W14lmnD-gu2U4dRjqIhy7JZpV9RUmv8-1ybQ92w"

    supabase_handler = SupabaseHandler(SUPABASE_URL, SUPABASE_KEY)
    scraper = FlipkartScraper(products, supabase_handler)
    scraper.scrape_all_products()

"""Amazon Sponsored Listings scraper using Playwright."""
import asyncio
from typing import List, Optional, Dict, Any
from datetime import datetime
from urllib.parse import quote

from playwright.async_api import async_playwright, Page, Browser
from bs4 import BeautifulSoup

from .models import AdCreative, AdSource, Lead
from .utils import (
    RateLimiter, retry_on_exception, setup_logger,
    extract_domain, clean_company_name, detect_captcha_block,
    get_random_user_agent
)

logger = setup_logger(__name__)


class AmazonAdsScraper:
    """Scraper for Amazon Sponsored Listings."""
    
    BASE_URL = "https://www.amazon.com/s"
    
    def __init__(self, rate_limiter: Optional[RateLimiter] = None, headless: bool = True):
        self.rate_limiter = rate_limiter or RateLimiter(min_delay=3.0, max_delay=5.0)
        self.headless = headless
        self.browser: Optional[Browser] = None
        self.page: Optional[Page] = None
    
    async def __aenter__(self):
        """Async context manager entry."""
        playwright = await async_playwright().start()
        self.browser = await playwright.chromium.launch(headless=self.headless)
        
        # Create page with anti-detection measures
        context = await self.browser.new_context(
            user_agent=get_random_user_agent(),
            viewport={'width': 1920, 'height': 1080},
            locale='en-US'
        )
        
        self.page = await context.new_page()
        
        # Set additional headers
        await self.page.set_extra_http_headers({
            'Accept-Language': 'en-US,en;q=0.9',
            'Accept-Encoding': 'gzip, deflate, br',
            'Cache-Control': 'no-cache'
        })
        
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        if self.page:
            await self.page.close()
        if self.browser:
            await self.browser.close()
    
    @retry_on_exception(max_attempts=2, delay=5.0)
    async def search_sponsored_products(
        self,
        search_queries: List[str],
        max_results_per_query: int = 20
    ) -> List[Lead]:
        """Search for sponsored products on Amazon."""
        if not self.page:
            raise RuntimeError("Browser not initialized. Use async context manager.")
        
        all_leads: Dict[str, Lead] = {}
        
        for query in search_queries:
            logger.info(f"Searching Amazon for: {query}")
            leads = await self._search_single_query(query, max_results_per_query)
            
            # Merge leads
            for lead in leads:
                if lead.domain in all_leads:
                    all_leads[lead.domain].merge_with(lead)
                else:
                    all_leads[lead.domain] = lead
        
        return list(all_leads.values())
    
    async def _search_single_query(self, query: str, max_results: int) -> List[Lead]:
        """Search for a single query."""
        leads = []
        
        # Build search URL
        params = {
            'k': query,
            'ref': 'nb_sb_noss'
        }
        url = f"{self.BASE_URL}?{'&'.join(f'{k}={quote(str(v))}' for k, v in params.items())}"
        
        self.rate_limiter.wait()
        
        try:
            # Navigate to search page
            await self.page.goto(url, wait_until='networkidle', timeout=30000)
            
            # Wait for results to load
            await self.page.wait_for_selector('[data-component-type="s-search-result"]', timeout=10000)
            
            # Check for CAPTCHA
            page_content = await self.page.content()
            if detect_captcha_block(page_content):
                logger.warning("CAPTCHA detected on Amazon. Skipping.")
                return leads
            
            # Extract sponsored products
            sponsored_selectors = [
                '[data-component-type="sp-sponsored-result"]',
                '[data-component-type="s-search-result"]:has-text("Sponsored")',
                '.s-result-item:has(.s-label-popover-default:has-text("Sponsored"))'
            ]
            
            for selector in sponsored_selectors:
                try:
                    elements = await self.page.query_selector_all(selector)
                    if elements:
                        logger.debug(f"Found {len(elements)} sponsored items with selector: {selector}")
                        break
                except:
                    continue
            else:
                logger.warning("No sponsored products found")
                return leads
            
            # Parse each sponsored product
            count = 0
            for element in elements:
                if count >= max_results:
                    break
                
                try:
                    # Extract product data
                    product_data = await self._extract_product_data(element)
                    if product_data:
                        lead = self._create_lead_from_product(product_data, query)
                        if lead:
                            leads.append(lead)
                            count += 1
                except Exception as e:
                    logger.error(f"Error extracting product data: {e}")
                    continue
            
            logger.info(f"Found {len(leads)} sponsored products for query: {query}")
            
        except Exception as e:
            logger.error(f"Error searching Amazon: {e}")
        
        return leads
    
    async def _extract_product_data(self, element) -> Optional[Dict[str, Any]]:
        """Extract data from a product element."""
        try:
            data = {}
            
            # Get product title
            title_element = await element.query_selector('h2 a span')
            if title_element:
                data['title'] = await title_element.inner_text()
            
            # Get brand
            brand_selectors = [
                '.s-size-mini.s-spacing-none.s-color-base',
                '[class*="a-size-base-plus"]',
                '.a-row.a-size-base.a-color-secondary'
            ]
            
            for selector in brand_selectors:
                brand_element = await element.query_selector(selector)
                if brand_element:
                    brand_text = await brand_element.inner_text()
                    if brand_text and len(brand_text) < 100:
                        data['brand'] = brand_text.strip()
                        break
            
            # Get product URL
            link_element = await element.query_selector('h2 a')
            if link_element:
                href = await link_element.get_attribute('href')
                if href:
                    data['product_url'] = f"https://www.amazon.com{href}" if href.startswith('/') else href
            
            # Get ASIN
            asin = await element.get_attribute('data-asin')
            if asin:
                data['asin'] = asin
            
            # Get price
            price_element = await element.query_selector('.a-price-whole')
            if price_element:
                price_text = await price_element.inner_text()
                data['price'] = price_text.strip()
            
            return data if data.get('brand') else None
            
        except Exception as e:
            logger.error(f"Error extracting product data: {e}")
            return None
    
    def _create_lead_from_product(self, product_data: Dict[str, Any], search_query: str) -> Optional[Lead]:
        """Create a Lead object from product data."""
        try:
            brand = product_data.get('brand', '').strip()
            if not brand:
                return None
            
            # Clean brand name
            brand = brand.replace('Visit the ', '').replace(' Store', '')
            brand = clean_company_name(brand)
            
            # Create pseudo-domain for brand
            domain = brand.lower().replace(' ', '').replace('.', '') + '.amazon'
            
            # Create ad creative
            creative = AdCreative(
                ad_id=product_data.get('asin'),
                advertiser_name=brand,
                landing_page_url=product_data.get('product_url'),
                source=AdSource.AMAZON_ADS,
                scraped_at=datetime.utcnow()
            )
            
            # Create lead
            lead = Lead(
                domain=domain,
                company_name=brand,
                first_seen=datetime.utcnow(),
                last_seen=datetime.utcnow(),
                sources=[AdSource.AMAZON_ADS],
                ad_creatives=[creative]
            )
            
            return lead
            
        except Exception as e:
            logger.error(f"Error creating lead from product: {e}")
            return None


async def scrape_amazon_ads(
    search_queries: List[str],
    max_results_per_query: int = 20,
    headless: bool = True
) -> List[Lead]:
    """Convenience function to scrape Amazon ads."""
    async with AmazonAdsScraper(headless=headless) as scraper:
        return await scraper.search_sponsored_products(
            search_queries=search_queries,
            max_results_per_query=max_results_per_query
        )

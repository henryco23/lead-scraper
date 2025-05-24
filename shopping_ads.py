"""Google Shopping Ads scraper using Playwright."""
import asyncio
from typing import List, Optional, Dict, Any
from datetime import datetime
from urllib.parse import quote

from playwright.async_api import async_playwright, Page, Browser

from .models import AdCreative, AdSource, Lead
from .utils import (
    RateLimiter, retry_on_exception, setup_logger,
    extract_domain, clean_company_name, detect_captcha_block,
    get_random_user_agent
)

logger = setup_logger(__name__)


class ShoppingAdsScraper:
    """Scraper for Google Shopping Ads."""
    
    BASE_URL = "https://www.google.com/search"
    
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
            locale='en-US',
            geolocation={'latitude': 45.5152, 'longitude': -122.6784},  # Portland, OR
            permissions=['geolocation']
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
    async def search_shopping_ads(
        self,
        search_queries: List[str],
        max_results_per_query: int = 20
    ) -> List[Lead]:
        """Search for shopping ads on Google."""
        if not self.page:
            raise RuntimeError("Browser not initialized. Use async context manager.")
        
        all_leads: Dict[str, Lead] = {}
        
        for query in search_queries:
            logger.info(f"Searching Google Shopping for: {query}")
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
        
        # Build search URL for shopping results
        params = {
            'q': query,
            'tbm': 'shop',  # Shopping results
            'hl': 'en',
            'gl': 'us'
        }
        url = f"{self.BASE_URL}?{'&'.join(f'{k}={quote(str(v))}' for k, v in params.items())}"
        
        self.rate_limiter.wait()
        
        try:
            # Navigate to search page
            await self.page.goto(url, wait_until='networkidle', timeout=30000)
            
            # Wait for results to load
            await self.page.wait_for_selector('.sh-dgr__content', timeout=10000)
            
            # Check for CAPTCHA
            page_content = await self.page.content()
            if detect_captcha_block(page_content):
                logger.warning("CAPTCHA detected on Google Shopping. Skipping.")
                return leads
            
            # Look for sponsored shopping results
            # Google Shopping ads are usually marked with "Sponsored" or have specific attributes
            sponsored_selectors = [
                '.sh-dgr__content:has(.sh-sp__pswtr)',  # Sponsored label
                '.sh-dgr__content:has-text("Sponsored")',
                '[data-docid]:has(.sh-sp__pswtr)',
                '.sh-pr__product:has(.sh-sp__pswtr)'
            ]
            
            all_products = []
            for selector in sponsored_selectors:
                try:
                    elements = await self.page.query_selector_all(selector)
                    if elements:
                        all_products.extend(elements)
                        logger.debug(f"Found {len(elements)} sponsored items with selector: {selector}")
                except:
                    continue
            
            if not all_products:
                logger.warning("No sponsored shopping products found")
                return leads
            
            # Parse each sponsored product
            count = 0
            seen_merchants = set()
            
            for element in all_products:
                if count >= max_results:
                    break
                
                try:
                    # Extract product data
                    product_data = await self._extract_product_data(element)
                    if product_data:
                        merchant = product_data.get('merchant', '')
                        # Skip if we've already seen this merchant
                        if merchant and merchant not in seen_merchants:
                            lead = self._create_lead_from_product(product_data, query)
                            if lead:
                                leads.append(lead)
                                seen_merchants.add(merchant)
                                count += 1
                except Exception as e:
                    logger.error(f"Error extracting product data: {e}")
                    continue
            
            logger.info(f"Found {len(leads)} unique merchants with sponsored products for query: {query}")
            
        except Exception as e:
            logger.error(f"Error searching Google Shopping: {e}")
        
        return leads
    
    async def _extract_product_data(self, element) -> Optional[Dict[str, Any]]:
        """Extract data from a shopping product element."""
        try:
            data = {}
            
            # Get product title
            title_element = await element.query_selector('.sh-dgr__grid-result h3')
            if title_element:
                data['title'] = await title_element.inner_text()
            
            # Get merchant/store name
            merchant_selectors = [
                '.sh-dgr__merchant-name',
                '.aULzUe',  # Alternative merchant selector
                '[data-merchant-name]'
            ]
            
            for selector in merchant_selectors:
                merchant_element = await element.query_selector(selector)
                if merchant_element:
                    merchant_text = await merchant_element.inner_text()
                    if merchant_text:
                        data['merchant'] = merchant_text.strip()
                        break
            
            # Get merchant domain if available
            link_element = await element.query_selector('a[href*="url?q="]')
            if link_element:
                href = await link_element.get_attribute('href')
                if href and 'url?q=' in href:
                    # Extract actual URL from Google redirect
                    import re
                    match = re.search(r'url\?q=([^&]+)', href)
                    if match:
                        merchant_url = match.group(1)
                        domain = extract_domain(merchant_url)
                        if domain:
                            data['domain'] = domain
            
            # Get price
            price_element = await element.query_selector('.sh-dgr__grid-result span[aria-label*="price"]')
            if price_element:
                data['price'] = await price_element.inner_text()
            
            # Get product URL
            product_link = await element.query_selector('a.sh-dgr__grid-result')
            if product_link:
                href = await product_link.get_attribute('href')
                if href:
                    data['product_url'] = f"https://www.google.com{href}" if href.startswith('/') else href
            
            return data if data.get('merchant') else None
            
        except Exception as e:
            logger.error(f"Error extracting shopping product data: {e}")
            return None
    
    def _create_lead_from_product(self, product_data: Dict[str, Any], search_query: str) -> Optional[Lead]:
        """Create a Lead object from shopping product data."""
        try:
            merchant = product_data.get('merchant', '').strip()
            if not merchant:
                return None
            
            # Use extracted domain or create pseudo-domain
            domain = product_data.get('domain')
            if not domain:
                # Create pseudo-domain from merchant name
                domain = merchant.lower().replace(' ', '').replace('.', '') + '.shopping'
            
            # Clean merchant name
            merchant = clean_company_name(merchant)
            
            # Create ad creative
            creative = AdCreative(
                advertiser_name=merchant,
                landing_page_url=product_data.get('product_url'),
                source=AdSource.SHOPPING_ADS,
                scraped_at=datetime.utcnow()
            )
            
            # Create lead
            lead = Lead(
                domain=domain,
                company_name=merchant,
                first_seen=datetime.utcnow(),
                last_seen=datetime.utcnow(),
                sources=[AdSource.SHOPPING_ADS],
                ad_creatives=[creative]
            )
            
            return lead
            
        except Exception as e:
            logger.error(f"Error creating lead from shopping product: {e}")
            return None


async def scrape_shopping_ads(
    search_queries: List[str],
    max_results_per_query: int = 20,
    headless: bool = True
) -> List[Lead]:
    """Convenience function to scrape Google Shopping ads."""
    async with ShoppingAdsScraper(headless=headless) as scraper:
        return await scraper.search_shopping_ads(
            search_queries=search_queries,
            max_results_per_query=max_results_per_query
        )

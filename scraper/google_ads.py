"""Google Ads Transparency Center scraper."""
import asyncio
import json
from typing import List, Optional, Dict, Any
from datetime import datetime, timedelta
import aiohttp
from urllib.parse import quote

from .models import AdCreative, AdSource, Lead
from .utils import (
    RateLimiter, retry_on_exception, setup_logger,
    extract_domain, clean_company_name, get_random_user_agent
)

logger = setup_logger(__name__)


class GoogleAdsScraper:
    """Scraper for Google Ads Transparency Center."""
    
    BASE_URL = "https://transparencyreport.google.com/transparencyreport/api/v3/ads/creatives"
    
    def __init__(self, rate_limiter: Optional[RateLimiter] = None):
        self.rate_limiter = rate_limiter or RateLimiter(min_delay=2.0, max_delay=4.0)
        self.session: Optional[aiohttp.ClientSession] = None
    
    async def __aenter__(self):
        """Async context manager entry."""
        headers = {
            'User-Agent': get_random_user_agent(),
            'Accept': 'application/json',
            'Accept-Language': 'en-US,en;q=0.9',
            'Referer': 'https://transparencyreport.google.com/political-ads/home'
        }
        self.session = aiohttp.ClientSession(headers=headers)
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        if self.session:
            await self.session.close()
    
    @retry_on_exception(max_attempts=3, delay=2.0)
    async def search_advertisers(
        self,
        query: str = "",
        region: str = "US",
        start_date: Optional[datetime] = None,
        max_results: int = 100
    ) -> List[Lead]:
        """Search for advertisers in Google Ads Transparency Center."""
        if not self.session:
            raise RuntimeError("Session not initialized. Use async context manager.")
        
        leads = []
        
        # Default to last 30 days if no start date
        if not start_date:
            start_date = datetime.utcnow() - timedelta(days=30)
        
        # Format dates for API
        start_str = start_date.strftime("%Y%m%d")
        end_str = datetime.utcnow().strftime("%Y%m%d")
        
        # Build query parameters
        params = {
            'entity_type': 'ADVERTISER',
            'region': region,
            'start_date': start_str,
            'end_date': end_str,
            'page_size': min(max_results, 50),  # API limit
            'page_token': ''
        }
        
        if query:
            params['query'] = query
        
        total_fetched = 0
        page_token = ''
        
        while total_fetched < max_results:
            self.rate_limiter.wait()
            
            if page_token:
                params['page_token'] = page_token
            
            try:
                async with self.session.get(self.BASE_URL, params=params) as response:
                    if response.status != 200:
                        logger.error(f"Google Ads API error: {response.status}")
                        break
                    
                    data = await response.json()
                    
                    # Parse advertisers from response
                    advertisers = data.get('advertisers', [])
                    if not advertisers:
                        logger.info("No more advertisers found")
                        break
                    
                    for advertiser in advertisers:
                        lead = self._parse_advertiser(advertiser)
                        if lead:
                            leads.append(lead)
                            total_fetched += 1
                            
                            if total_fetched >= max_results:
                                break
                    
                    # Check for next page
                    page_token = data.get('next_page_token', '')
                    if not page_token:
                        break
                        
            except Exception as e:
                logger.error(f"Error fetching Google Ads data: {e}")
                break
        
        logger.info(f"Found {len(leads)} advertisers from Google Ads")
        return leads
    
    def _parse_advertiser(self, advertiser_data: Dict[str, Any]) -> Optional[Lead]:
        """Parse advertiser data into Lead object."""
        try:
            # Extract basic info
            advertiser_name = advertiser_data.get('advertiser_name', '').strip()
            if not advertiser_name:
                return None
            
            # Extract domain from advertiser info or ads
            domain = None
            landing_urls = []
            
            # Try to get domain from advertiser metadata
            advertiser_url = advertiser_data.get('advertiser_url', '')
            if advertiser_url:
                domain = extract_domain(advertiser_url)
            
            # Parse ad creatives
            creatives = []
            for ad in advertiser_data.get('ads', []):
                creative = self._parse_ad_creative(ad, advertiser_name)
                if creative:
                    creatives.append(creative)
                    if creative.landing_page_url:
                        landing_urls.append(str(creative.landing_page_url))
            
            # If no domain from advertiser, try to extract from ads
            if not domain and landing_urls:
                for url in landing_urls:
                    domain = extract_domain(url)
                    if domain:
                        break
            
            if not domain:
                logger.warning(f"No domain found for advertiser: {advertiser_name}")
                return None
            
            # Calculate metrics
            total_impressions = sum(c.impressions or 0 for c in creatives)
            total_spend = sum(c.spend_estimate or 0 for c in creatives)
            
            # Create lead
            lead = Lead(
                domain=domain,
                company_name=clean_company_name(advertiser_name),
                first_seen=datetime.utcnow(),
                last_seen=datetime.utcnow(),
                sources=[AdSource.GOOGLE_ADS],
                ad_creatives=creatives,
                total_impressions=total_impressions if total_impressions > 0 else None,
                total_spend_estimate=total_spend if total_spend > 0 else None
            )
            
            return lead
            
        except Exception as e:
            logger.error(f"Error parsing advertiser data: {e}")
            return None
    
    def _parse_ad_creative(self, ad_data: Dict[str, Any], advertiser_name: str) -> Optional[AdCreative]:
        """Parse individual ad creative."""
        try:
            creative = AdCreative(
                ad_id=ad_data.get('ad_id'),
                advertiser_name=advertiser_name,
                creative_url=ad_data.get('creative_url'),
                landing_page_url=ad_data.get('landing_page_url'),
                source=AdSource.GOOGLE_ADS
            )
            
            # Parse date if available
            if 'first_shown' in ad_data:
                try:
                    date_str = ad_data['first_shown']
                    creative.campaign_start_date = datetime.strptime(date_str, "%Y%m%d")
                except:
                    pass
            
            # Parse metrics
            if 'impressions_range' in ad_data:
                # Google provides ranges, take midpoint
                imp_range = ad_data['impressions_range']
                if isinstance(imp_range, dict):
                    min_imp = imp_range.get('min', 0)
                    max_imp = imp_range.get('max', min_imp)
                    creative.impressions = (min_imp + max_imp) // 2
            
            if 'spend_range' in ad_data:
                spend_range = ad_data['spend_range']
                if isinstance(spend_range, dict):
                    min_spend = spend_range.get('min', 0)
                    max_spend = spend_range.get('max', min_spend)
                    creative.spend_estimate = (min_spend + max_spend) / 2
            
            return creative
            
        except Exception as e:
            logger.error(f"Error parsing ad creative: {e}")
            return None


async def scrape_google_ads(
    query: str = "",
    region: str = "US", 
    start_date: Optional[datetime] = None,
    max_results: int = 100
) -> List[Lead]:
    """Convenience function to scrape Google Ads."""
    async with GoogleAdsScraper() as scraper:
        return await scraper.search_advertisers(
            query=query,
            region=region,
            start_date=start_date,
            max_results=max_results
        )

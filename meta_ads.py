"""Meta Ad Library scraper."""
import asyncio
import os
from typing import List, Optional, Dict, Any
from datetime import datetime
import aiohttp

from .models import AdCreative, AdSource, Lead
from .utils import (
    RateLimiter, retry_on_exception, setup_logger,
    extract_domain, clean_company_name, get_random_user_agent
)

logger = setup_logger(__name__)


class MetaAdsScraper:
    """Scraper for Meta Ad Library."""
    
    BASE_URL = "https://graph.facebook.com/v18.0/ads_archive"
    
    def __init__(self, access_token: Optional[str] = None, rate_limiter: Optional[RateLimiter] = None):
        self.access_token = access_token or os.environ.get('META_ACCESS_TOKEN', '')
        self.rate_limiter = rate_limiter or RateLimiter(min_delay=1.0, max_delay=2.0)
        self.session: Optional[aiohttp.ClientSession] = None
        
        if not self.access_token:
            logger.warning("No Meta access token provided. Set META_ACCESS_TOKEN env var.")
    
    async def __aenter__(self):
        """Async context manager entry."""
        headers = {
            'User-Agent': get_random_user_agent(),
            'Accept': 'application/json'
        }
        self.session = aiohttp.ClientSession(headers=headers)
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        if self.session:
            await self.session.close()
    
    @retry_on_exception(max_attempts=3, delay=2.0)
    async def search_ads(
        self,
        search_terms: str = "",
        ad_reached_countries: str = "US",
        ad_active_status: str = "ACTIVE",
        limit: int = 100
    ) -> List[Lead]:
        """Search for ads in Meta Ad Library."""
        if not self.session:
            raise RuntimeError("Session not initialized. Use async context manager.")
        
        if not self.access_token:
            logger.error("No Meta access token available")
            return []
        
        leads_map: Dict[str, Lead] = {}
        
        params = {
            'access_token': self.access_token,
            'ad_reached_countries': ad_reached_countries,
            'ad_active_status': ad_active_status,
            'fields': 'id,ad_creation_time,ad_creative_bodies,ad_creative_link_captions,'
                     'ad_creative_link_descriptions,ad_creative_link_titles,ad_snapshot_url,'
                     'page_id,page_name,impressions,spend,currency,ad_delivery_start_time',
            'limit': min(limit, 100)  # API max
        }
        
        if search_terms:
            params['search_terms'] = search_terms
        
        total_fetched = 0
        next_url = self.BASE_URL
        
        while total_fetched < limit and next_url:
            self.rate_limiter.wait()
            
            try:
                async with self.session.get(next_url, params=params if next_url == self.BASE_URL else None) as response:
                    if response.status != 200:
                        error_data = await response.text()
                        logger.error(f"Meta API error: {response.status} - {error_data}")
                        break
                    
                    data = await response.json()
                    ads = data.get('data', [])
                    
                    if not ads:
                        logger.info("No more ads found")
                        break
                    
                    for ad in ads:
                        lead = self._parse_ad(ad)
                        if lead:
                            # Merge with existing lead if same domain
                            if lead.domain in leads_map:
                                leads_map[lead.domain].merge_with(lead)
                            else:
                                leads_map[lead.domain] = lead
                            
                            total_fetched += 1
                            if total_fetched >= limit:
                                break
                    
                    # Check for next page
                    paging = data.get('paging', {})
                    next_url = paging.get('next')
                    
                    # Clear params for pagination URLs
                    params = None
                    
            except Exception as e:
                logger.error(f"Error fetching Meta ads: {e}")
                break
        
        leads = list(leads_map.values())
        logger.info(f"Found {len(leads)} unique advertisers from Meta")
        return leads
    
    def _parse_ad(self, ad_data: Dict[str, Any]) -> Optional[Lead]:
        """Parse Meta ad data into Lead object."""
        try:
            # Extract advertiser info
            page_name = ad_data.get('page_name', '').strip()
            if not page_name:
                return None
            
            # Try to extract domain from ad
            domain = None
            ad_snapshot_url = ad_data.get('ad_snapshot_url', '')
            
            # Look for domain in various fields
            link_captions = ad_data.get('ad_creative_link_captions', [])
            link_descriptions = ad_data.get('ad_creative_link_descriptions', [])
            link_titles = ad_data.get('ad_creative_link_titles', [])
            
            # Try to find domain in link texts
            for texts in [link_captions, link_descriptions, link_titles]:
                if texts and isinstance(texts, list):
                    for text in texts:
                        if '.' in text and len(text) < 50:  # Might be a domain
                            potential_domain = extract_domain(text)
                            if potential_domain:
                                domain = potential_domain
                                break
                if domain:
                    break
            
            # If no domain found, use page name as company name
            if not domain:
                # Create a pseudo-domain from page name
                domain = page_name.lower().replace(' ', '') + '.meta'
                logger.debug(f"No domain found for {page_name}, using pseudo-domain: {domain}")
            
            # Create ad creative
            creative = AdCreative(
                ad_id=ad_data.get('id'),
                advertiser_name=page_name,
                creative_url=ad_snapshot_url if ad_snapshot_url else None,
                source=AdSource.META_ADS
            )
            
            # Parse dates
            if 'ad_creation_time' in ad_data:
                try:
                    creative.campaign_start_date = datetime.fromisoformat(
                        ad_data['ad_creation_time'].replace('Z', '+00:00')
                    )
                except:
                    pass
            
            # Parse metrics
            impressions_data = ad_data.get('impressions')
            if impressions_data:
                if isinstance(impressions_data, dict):
                    # Range format
                    lower = int(impressions_data.get('lower_bound', 0))
                    upper = int(impressions_data.get('upper_bound', lower))
                    creative.impressions = (lower + upper) // 2
                elif isinstance(impressions_data, str):
                    # Try to parse string format
                    try:
                        creative.impressions = int(impressions_data.replace(',', ''))
                    except:
                        pass
            
            spend_data = ad_data.get('spend')
            if spend_data:
                if isinstance(spend_data, dict):
                    lower = float(spend_data.get('lower_bound', 0))
                    upper = float(spend_data.get('upper_bound', lower))
                    creative.spend_estimate = (lower + upper) / 2
                elif isinstance(spend_data, str):
                    try:
                        creative.spend_estimate = float(spend_data.replace(',', ''))
                    except:
                        pass
            
            # Create lead
            lead = Lead(
                domain=domain,
                company_name=clean_company_name(page_name),
                first_seen=creative.campaign_start_date or datetime.utcnow(),
                last_seen=datetime.utcnow(),
                sources=[AdSource.META_ADS],
                ad_creatives=[creative],
                total_impressions=creative.impressions,
                total_spend_estimate=creative.spend_estimate
            )
            
            return lead
            
        except Exception as e:
            logger.error(f"Error parsing Meta ad: {e}")
            return None


async def scrape_meta_ads(
    search_terms: str = "",
    ad_reached_countries: str = "US",
    ad_active_status: str = "ACTIVE",
    limit: int = 100,
    access_token: Optional[str] = None
) -> List[Lead]:
    """Convenience function to scrape Meta ads."""
    async with MetaAdsScraper(access_token=access_token) as scraper:
        return await scraper.search_ads(
            search_terms=search_terms,
            ad_reached_countries=ad_reached_countries,
            ad_active_status=ad_active_status,
            limit=limit
        )

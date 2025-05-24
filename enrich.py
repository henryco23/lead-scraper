"""Company enrichment module for extracting additional information."""
import asyncio
import os
import re
from typing import Optional, Dict, Any, List
from urllib.parse import urljoin

import aiohttp
from bs4 import BeautifulSoup

from .models import CompanyInfo, Lead
from .utils import (
    RateLimiter, retry_on_exception, setup_logger,
    get_random_user_agent, extract_email_from_text,
    extract_phone_from_text, normalize_url
)

logger = setup_logger(__name__)


class CompanyEnricher:
    """Enrich leads with company information from their websites."""
    
    def __init__(
        self,
        clearbit_api_key: Optional[str] = None,
        rate_limiter: Optional[RateLimiter] = None
    ):
        self.clearbit_api_key = clearbit_api_key or os.environ.get('CLEARBIT_API_KEY')
        self.rate_limiter = rate_limiter or RateLimiter(min_delay=1.0, max_delay=2.0)
        self.session: Optional[aiohttp.ClientSession] = None
    
    async def __aenter__(self):
        """Async context manager entry."""
        headers = {
            'User-Agent': get_random_user_agent(),
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.9'
        }
        
        timeout = aiohttp.ClientTimeout(total=30)
        self.session = aiohttp.ClientSession(headers=headers, timeout=timeout)
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        if self.session:
            await self.session.close()
    
    async def enrich_leads(self, leads: List[Lead]) -> List[Lead]:
        """Enrich multiple leads with company information."""
        logger.info(f"Enriching {len(leads)} leads...")
        
        tasks = []
        for lead in leads:
            if not lead.company_info:
                task = self.enrich_single_lead(lead)
                tasks.append(task)
        
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        
        return leads
    
    @retry_on_exception(max_attempts=2, delay=1.0)
    async def enrich_single_lead(self, lead: Lead) -> Lead:
        """Enrich a single lead with company information."""
        if not self.session:
            raise RuntimeError("Session not initialized. Use async context manager.")
        
        company_info = CompanyInfo()
        
        # Try Clearbit API first if available
        if self.clearbit_api_key and not lead.domain.endswith(('.meta', '.amazon', '.shopping')):
            clearbit_data = await self._fetch_clearbit_data(lead.domain)
            if clearbit_data:
                company_info = self._parse_clearbit_data(clearbit_data)
        
        # Fetch additional data from website
        if not lead.domain.endswith(('.meta', '.amazon', '.shopping')):
            website_data = await self._fetch_website_data(lead.domain)
            if website_data:
                # Merge website data with Clearbit data
                website_info = self._parse_website_data(website_data)
                for field, value in website_info.dict(exclude_unset=True).items():
                    if value and not getattr(company_info, field):
                        setattr(company_info, field, value)
        
        # Only set company info if we found something
        if company_info.dict(exclude_unset=True):
            lead.company_info = company_info
            logger.debug(f"Enriched {lead.domain} with company info")
        
        return lead
    
    async def _fetch_clearbit_data(self, domain: str) -> Optional[Dict[str, Any]]:
        """Fetch company data from Clearbit API."""
        if not self.clearbit_api_key:
            return None
        
        url = f"https://company.clearbit.com/v2/companies/find?domain={domain}"
        headers = {'Authorization': f'Bearer {self.clearbit_api_key}'}
        
        self.rate_limiter.wait()
        
        try:
            async with self.session.get(url, headers=headers) as response:
                if response.status == 200:
                    return await response.json()
                elif response.status == 404:
                    logger.debug(f"No Clearbit data found for {domain}")
                else:
                    logger.warning(f"Clearbit API error: {response.status}")
        except Exception as e:
            logger.error(f"Error fetching Clearbit data: {e}")
        
        return None
    
    def _parse_clearbit_data(self, data: Dict[str, Any]) -> CompanyInfo:
        """Parse Clearbit API response into CompanyInfo."""
        info = CompanyInfo()
        
        # Extract relevant fields
        if 'name' in data:
            info.website_title = data['name']
        
        if 'linkedin' in data and data['linkedin'].get('handle'):
            info.linkedin_url = f"https://www.linkedin.com/company/{data['linkedin']['handle']}"
        
        if 'phone' in data:
            info.phone = data['phone']
        
        if 'emailProvider' in data and data.get('domain'):
            # Try to construct a generic contact email
            if data['emailProvider']:
                info.email = f"info@{data['domain']}"
        
        if 'metrics' in data:
            metrics = data['metrics']
            if 'employees' in metrics:
                info.company_size = str(metrics['employees'])
            elif 'employeesRange' in metrics:
                info.company_size = metrics['employeesRange']
        
        if 'category' in data and data['category'].get('industry'):
            info.industry = data['category']['industry']
        
        return info
    
    async def _fetch_website_data(self, domain: str) -> Optional[Dict[str, Any]]:
        """Fetch data from company website."""
        url = normalize_url(domain)
        
        self.rate_limiter.wait()
        
        try:
            async with self.session.get(url, allow_redirects=True) as response:
                if response.status != 200:
                    logger.debug(f"Failed to fetch {url}: {response.status}")
                    return None
                
                html = await response.text()
                return {
                    'html': html,
                    'final_url': str(response.url)
                }
        except Exception as e:
            logger.debug(f"Error fetching website {url}: {e}")
            return None
    
    def _parse_website_data(self, data: Dict[str, Any]) -> CompanyInfo:
        """Parse website HTML to extract company information."""
        info = CompanyInfo()
        html = data['html']
        
        try:
            soup = BeautifulSoup(html, 'html.parser')
            
            # Extract title
            title_tag = soup.find('title')
            if title_tag:
                info.website_title = title_tag.text.strip()
            
            # Look for LinkedIn URL
            linkedin_patterns = [
                r'linkedin\.com/company/([a-zA-Z0-9-]+)',
                r'linkedin\.com/in/([a-zA-Z0-9-]+)'
            ]
            
            for link in soup.find_all('a', href=True):
                href = link['href']
                for pattern in linkedin_patterns:
                    if re.search(pattern, href):
                        info.linkedin_url = href
                        break
                if info.linkedin_url:
                    break
            
            # Extract contact information
            # Look in common contact sections
            contact_sections = soup.find_all(['div', 'section', 'footer'], 
                                            class_=re.compile(r'contact|footer', re.I))
            
            contact_text = ' '.join(section.get_text() for section in contact_sections)
            
            # Also check the entire page for contact info
            full_text = soup.get_text()
            
            # Extract email
            if not info.email:
                email = extract_email_from_text(contact_text) or extract_email_from_text(full_text)
                if email:
                    info.email = email
            
            # Extract phone
            if not info.phone:
                phone = extract_phone_from_text(contact_text) or extract_phone_from_text(full_text)
                if phone:
                    info.phone = phone
            
            # Look for company size indicators
            size_patterns = [
                (r'(\d+)\+?\s*employees', lambda m: f"{m.group(1)}+ employees"),
                (r'team of (\d+)', lambda m: f"~{m.group(1)} employees"),
                (r'(\d+)-(\d+)\s*employees', lambda m: f"{m.group(1)}-{m.group(2)} employees")
            ]
            
            for pattern, formatter in size_patterns:
                match = re.search(pattern, full_text, re.I)
                if match:
                    info.company_size = formatter(match)
                    break
            
        except Exception as e:
            logger.error(f"Error parsing website data: {e}")
        
        return info


async def enrich_leads(
    leads: List[Lead],
    clearbit_api_key: Optional[str] = None
) -> List[Lead]:
    """Convenience function to enrich leads."""
    async with CompanyEnricher(clearbit_api_key=clearbit_api_key) as enricher:
        return await enricher.enrich_leads(leads)

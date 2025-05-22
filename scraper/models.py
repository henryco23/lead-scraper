"""Pydantic models for lead scraper data structures."""
from datetime import datetime
from typing import Optional, List
from pydantic import BaseModel, HttpUrl, EmailStr, Field
from enum import Enum


class AdSource(str, Enum):
    """Enum for ad platforms."""
    GOOGLE_ADS = "google_ads"
    META_ADS = "meta_ads"
    AMAZON_ADS = "amazon_ads"
    SHOPPING_ADS = "shopping_ads"


class AdCreative(BaseModel):
    """Model for ad creative data."""
    ad_id: Optional[str] = None
    advertiser_name: str
    creative_url: Optional[HttpUrl] = None
    campaign_start_date: Optional[datetime] = None
    impressions: Optional[int] = None
    spend_estimate: Optional[float] = None
    landing_page_url: Optional[HttpUrl] = None
    source: AdSource
    scraped_at: datetime = Field(default_factory=datetime.utcnow)


class CompanyInfo(BaseModel):
    """Model for enriched company information."""
    website_title: Optional[str] = None
    linkedin_url: Optional[HttpUrl] = None
    phone: Optional[str] = None
    email: Optional[EmailStr] = None
    company_size: Optional[str] = None
    industry: Optional[str] = None


class Lead(BaseModel):
    """Model for a deduplicated lead record."""
    domain: str
    company_name: str
    first_seen: datetime
    last_seen: datetime
    sources: List[AdSource] = Field(default_factory=list)
    ad_creatives: List[AdCreative] = Field(default_factory=list)
    company_info: Optional[CompanyInfo] = None
    total_impressions: Optional[int] = None
    total_spend_estimate: Optional[float] = None
    is_active: bool = True
    
    def merge_with(self, other: 'Lead') -> None:
        """Merge another lead's data into this one."""
        self.last_seen = max(self.last_seen, other.last_seen)
        self.first_seen = min(self.first_seen, other.first_seen)
        
        # Merge sources
        for source in other.sources:
            if source not in self.sources:
                self.sources.append(source)
        
        # Merge ad creatives
        existing_ad_ids = {ad.ad_id for ad in self.ad_creatives if ad.ad_id}
        for ad in other.ad_creatives:
            if not ad.ad_id or ad.ad_id not in existing_ad_ids:
                self.ad_creatives.append(ad)
        
        # Update company info if better data available
        if other.company_info and (not self.company_info or 
                                    other.company_info.dict(exclude_unset=True)):
            if not self.company_info:
                self.company_info = other.company_info
            else:
                # Merge non-null fields
                for field, value in other.company_info.dict(exclude_unset=True).items():
                    if value and not getattr(self.company_info, field):
                        setattr(self.company_info, field, value)
        
        # Update metrics
        if other.total_impressions:
            self.total_impressions = (self.total_impressions or 0) + other.total_impressions
        if other.total_spend_estimate:
            self.total_spend_estimate = (self.total_spend_estimate or 0) + other.total_spend_estimate


class ScrapeResult(BaseModel):
    """Model for scrape operation results."""
    source: AdSource
    success: bool
    leads_found: int = 0
    errors: List[str] = Field(default_factory=list)
    duration_seconds: float = 0

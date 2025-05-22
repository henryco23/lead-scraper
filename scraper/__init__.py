"""Multi-source lead scraper for B2B sales teams."""

__version__ = "1.0.0"

from .models import Lead, AdCreative, CompanyInfo, AdSource
from .db import LeadDatabase
from .google_ads import scrape_google_ads
from .meta_ads import scrape_meta_ads
from .amazon_ads import scrape_amazon_ads
from .shopping_ads import scrape_shopping_ads
from .enrich import enrich_leads

__all__ = [
    "Lead",
    "AdCreative", 
    "CompanyInfo",
    "AdSource",
    "LeadDatabase",
    "scrape_google_ads",
    "scrape_meta_ads",
    "scrape_amazon_ads",
    "scrape_shopping_ads",
    "enrich_leads"
]

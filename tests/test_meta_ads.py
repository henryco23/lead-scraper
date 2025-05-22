"""Tests for Meta Ads scraper."""
import pytest
from datetime import datetime
from unittest.mock import Mock, patch, AsyncMock

from scraper.models import AdSource, Lead
from scraper.meta_ads import MetaAdsScraper, scrape_meta_ads


class TestMetaAdsScraper:
    """Test cases for Meta Ads scraper."""
    
    @pytest.fixture
    def mock_response_data(self):
        """Mock Meta Ad Library API response."""
        return {
            "data": [
                {
                    "id": "123456789",
                    "page_name": "Digital Marketing Pro",
                    "ad_snapshot_url": "https://facebook.com/ads/archive/render/?id=123456789",
                    "ad_creation_time": "2025-05-01T10:00:00Z",
                    "ad_creative_link_captions": ["Visit digitalmarketingpro.com"],
                    "impressions": {
                        "lower_bound": 1000,
                        "upper_bound": 5000
                    },
                    "spend": {
                        "lower_bound": 100,
                        "upper_bound": 500
                    }
                },
                {
                    "id": "987654321",
                    "page_name": "SaaS Solutions",
                    "ad_creation_time": "2025-05-15T14:30:00Z",
                    "impressions": "10000",
                    "spend": "1000"
                }
            ],
            "paging": {
                "next": None
            }
        }
    
    @pytest.mark.asyncio
    async def test_search_ads_with_token(self, mock_response_data):
        """Test searching for ads with access token."""
        scraper = MetaAdsScraper(access_token="test_token")
        
        # Mock the session
        mock_session = AsyncMock()
        mock_response = AsyncMock()
        mock_response.status = 200
        mock_response.json = AsyncMock(return_value=mock_response_data)
        
        mock_session.get = AsyncMock(return_value=mock_response)
        mock_session.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session.__aexit__ = AsyncMock(return_value=None)
        
        scraper.session = mock_session
        
        # Test search
        leads = await scraper.search_ads(
            search_terms="marketing",
            limit=10
        )
        
        # Assertions
        assert len(leads) == 2
        
        # First lead (with domain from link caption)
        assert leads[0].company_name == "Digital Marketing Pro"
        assert leads[0].domain == "digitalmarketingpro.com"
        assert leads[0].sources == [AdSource.META_ADS]
        assert len(leads[0].ad_creatives) == 1
        assert leads[0].total_impressions == 3000  # midpoint
        assert leads[0].total_spend_estimate == 300  # midpoint
        
        # Second lead (pseudo-domain)
        assert leads[1].company_name == "SaaS Solutions"
        assert leads[1].domain == "saassolutions.meta"
        assert leads[1].total_impressions == 10000
        assert leads[1].total_spend_estimate == 1000
    
    @pytest.mark.asyncio
    async def test_search_ads_without_token(self):
        """Test searching without access token."""
        scraper = MetaAdsScraper(access_token=None)
        scraper.session = AsyncMock()
        
        leads = await scraper.search_ads()
        
        assert leads == []
    
    def test_parse_ad_with_domain_extraction(self):
        """Test parsing ad with domain extraction from captions."""
        scraper = MetaAdsScraper()
        
        ad_data = {
            "id": "123",
            "page_name": "Test Company",
            "ad_creative_link_captions": ["Check out testcompany.com today!"],
            "ad_creation_time": "2025-05-20T10:00:00Z",
            "impressions": {"lower_bound": 500, "upper_bound": 1000}
        }
        
        lead = scraper._parse_ad(ad_data)
        
        assert lead is not None
        assert lead.domain == "testcompany.com"
        assert lead.company_name == "Test Company"
        assert lead.ad_creatives[0].impressions == 750
    
    def test_parse_ad_without_page_name(self):
        """Test parsing ad without page name."""
        scraper = MetaAdsScraper()
        
        ad_data = {
            "id": "123",
            "ad_snapshot_url": "https://facebook.com/ads/123"
        }
        
        lead = scraper._parse_ad(ad_data)
        assert lead is None
    
    @pytest.mark.asyncio
    async def test_api_error_handling(self):
        """Test handling of API errors."""
        scraper = MetaAdsScraper(access_token="test_token")
        
        # Mock error response
        mock_session = AsyncMock()
        mock_response = AsyncMock()
        mock_response.status = 400
        mock_response.text = AsyncMock(return_value="Bad Request")
        
        mock_session.get = AsyncMock(return_value=mock_response)
        scraper.session = mock_session
        
        leads = await scraper.search_ads()
        assert leads == []
    
    @pytest.mark.asyncio
    async def test_scrape_meta_ads_function(self):
        """Test the convenience function."""
        with patch('scraper.meta_ads.MetaAdsScraper') as mock_scraper_class:
            mock_scraper = AsyncMock()
            mock_scraper.__aenter__ = AsyncMock(return_value=mock_scraper)
            mock_scraper.__aexit__ = AsyncMock(return_value=None)
            mock_scraper.search_ads = AsyncMock(return_value=[])
            
            mock_scraper_class.return_value = mock_scraper
            
            result = await scrape_meta_ads(
                search_terms="test",
                limit=50,
                access_token="test_token"
            )
            
            assert result == []
            mock_scraper.search_ads.assert_called_once()

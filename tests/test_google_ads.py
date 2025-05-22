"""Tests for Google Ads scraper."""
import pytest
from datetime import datetime, timedelta
from unittest.mock import Mock, patch, AsyncMock

from scraper.models import AdSource, Lead
from scraper.google_ads import GoogleAdsScraper, scrape_google_ads


class TestGoogleAdsScraper:
    """Test cases for Google Ads scraper."""
    
    @pytest.fixture
    def mock_response_data(self):
        """Mock Google Ads API response."""
        return {
            "advertisers": [
                {
                    "advertiser_name": "Tech Solutions Inc",
                    "advertiser_url": "https://techsolutions.com",
                    "ads": [
                        {
                            "ad_id": "123456",
                            "creative_url": "https://example.com/ad1.jpg",
                            "landing_page_url": "https://techsolutions.com/product",
                            "first_shown": "20250501",
                            "impressions_range": {"min": 10000, "max": 50000},
                            "spend_range": {"min": 1000, "max": 5000}
                        }
                    ]
                },
                {
                    "advertiser_name": "Software Corp",
                    "advertiser_url": "https://softwarecorp.com",
                    "ads": [
                        {
                            "ad_id": "789012",
                            "landing_page_url": "https://softwarecorp.com/demo",
                            "impressions_range": {"min": 5000, "max": 20000}
                        }
                    ]
                }
            ],
            "next_page_token": ""
        }
    
    @pytest.mark.asyncio
    async def test_search_advertisers(self, mock_response_data):
        """Test searching for advertisers."""
        scraper = GoogleAdsScraper()
        
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
        leads = await scraper.search_advertisers(
            query="software",
            max_results=10
        )
        
        # Assertions
        assert len(leads) == 2
        assert leads[0].company_name == "Tech Solutions Inc"
        assert leads[0].domain == "techsolutions.com"
        assert leads[0].sources == [AdSource.GOOGLE_ADS]
        assert len(leads[0].ad_creatives) == 1
        assert leads[0].total_impressions == 30000  # midpoint
        assert leads[0].total_spend_estimate == 3000  # midpoint
        
        assert leads[1].company_name == "Software Corp"
        assert leads[1].domain == "softwarecorp.com"
    
    def test_parse_advertiser_missing_domain(self):
        """Test parsing advertiser without domain."""
        scraper = GoogleAdsScraper()
        
        advertiser_data = {
            "advertiser_name": "No Domain Corp",
            "ads": []
        }
        
        lead = scraper._parse_advertiser(advertiser_data)
        assert lead is None
    
    def test_parse_ad_creative(self):
        """Test parsing individual ad creative."""
        scraper = GoogleAdsScraper()
        
        ad_data = {
            "ad_id": "123",
            "creative_url": "https://example.com/ad.jpg",
            "landing_page_url": "https://example.com/landing",
            "first_shown": "20250515",
            "impressions_range": {"min": 1000, "max": 2000},
            "spend_range": {"min": 100, "max": 200}
        }
        
        creative = scraper._parse_ad_creative(ad_data, "Test Company")
        
        assert creative is not None
        assert creative.ad_id == "123"
        assert creative.advertiser_name == "Test Company"
        assert creative.impressions == 1500
        assert creative.spend_estimate == 150
        assert creative.campaign_start_date.year == 2025
        assert creative.campaign_start_date.month == 5
        assert creative.campaign_start_date.day == 15
    
    @pytest.mark.asyncio
    async def test_scrape_google_ads_function(self):
        """Test the convenience function."""
        with patch('scraper.google_ads.GoogleAdsScraper') as mock_scraper_class:
            mock_scraper = AsyncMock()
            mock_scraper.__aenter__ = AsyncMock(return_value=mock_scraper)
            mock_scraper.__aexit__ = AsyncMock(return_value=None)
            mock_scraper.search_advertisers = AsyncMock(return_value=[])
            
            mock_scraper_class.return_value = mock_scraper
            
            result = await scrape_google_ads(query="test", max_results=50)
            
            assert result == []
            mock_scraper.search_advertisers.assert_called_once()

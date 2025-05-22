"""Tests for Amazon Ads scraper."""
import pytest
from datetime import datetime
from unittest.mock import Mock, patch, AsyncMock

from scraper.models import AdSource, Lead
from scraper.amazon_ads import AmazonAdsScraper, scrape_amazon_ads


class TestAmazonAdsScraper:
    """Test cases for Amazon Ads scraper."""
    
    @pytest.fixture
    def mock_page(self):
        """Create a mock Playwright page."""
        page = AsyncMock()
        
        # Mock basic page methods
        page.goto = AsyncMock()
        page.content = AsyncMock(return_value="<html>Normal page content</html>")
        page.wait_for_selector = AsyncMock()
        page.query_selector_all = AsyncMock()
        
        return page
    
    @pytest.fixture
    def mock_product_element(self):
        """Create a mock product element."""
        element = AsyncMock()
        
        # Title element
        title_elem = AsyncMock()
        title_elem.inner_text = AsyncMock(return_value="Premium Wireless Headphones")
        
        # Brand element
        brand_elem = AsyncMock()
        brand_elem.inner_text = AsyncMock(return_value="AudioTech Pro")
        
        # Link element
        link_elem = AsyncMock()
        link_elem.get_attribute = AsyncMock(return_value="/dp/B08XYZ123/ref=sr_1_1")
        
        # Price element
        price_elem = AsyncMock()
        price_elem.inner_text = AsyncMock(return_value="$99")
        
        # Element queries
        element.query_selector = AsyncMock(side_effect=lambda selector: {
            'h2 a span': title_elem,
            '.s-size-mini.s-spacing-none.s-color-base': brand_elem,
            'h2 a': link_elem,
            '.a-price-whole': price_elem
        }.get(selector))
        
        element.get_attribute = AsyncMock(return_value="B08XYZ123")
        
        return element
    
    @pytest.mark.asyncio
    async def test_search_sponsored_products(self, mock_page, mock_product_element):
        """Test searching for sponsored products."""
        scraper = AmazonAdsScraper()
        scraper.page = mock_page
        
        # Mock page behavior
        mock_page.query_selector_all.return_value = [mock_product_element]
        
        # Test search
        leads = await scraper.search_sponsored_products(
            search_queries=["headphones"],
            max_results_per_query=5
        )
        
        # Assertions
        assert len(leads) == 1
        assert leads[0].company_name == "AudioTech Pro"
        assert leads[0].domain == "audiotechpro.amazon"
        assert leads[0].sources == [AdSource.AMAZON_ADS]
        assert len(leads[0].ad_creatives) == 1
        assert leads[0].ad_creatives[0].ad_id == "B08XYZ123"
    
    @pytest.mark.asyncio
    async def test_captcha_detection(self, mock_page):
        """Test CAPTCHA detection and handling."""
        scraper = AmazonAdsScraper()
        scraper.page = mock_page
        
        # Mock CAPTCHA page
        mock_page.content.return_value = "<html>Please complete the captcha verification</html>"
        mock_page.query_selector_all.return_value = []
        
        leads = await scraper._search_single_query("test", 10)
        
        assert leads == []
    
    @pytest.mark.asyncio
    async def test_extract_product_data(self, mock_product_element):
        """Test extracting data from product element."""
        scraper = AmazonAdsScraper()
        
        data = await scraper._extract_product_data(mock_product_element)
        
        assert data is not None
        assert data['title'] == "Premium Wireless Headphones"
        assert data['brand'] == "AudioTech Pro"
        assert data['product_url'] == "https://www.amazon.com/dp/B08XYZ123/ref=sr_1_1"
        assert data['asin'] == "B08XYZ123"
        assert data['price'] == "$99"
    
    def test_create_lead_from_product(self):
        """Test creating lead from product data."""
        scraper = AmazonAdsScraper()
        
        product_data = {
            'brand': 'Visit the TechGear Store',
            'asin': 'B09ABC123',
            'product_url': 'https://www.amazon.com/dp/B09ABC123',
            'title': 'Wireless Mouse',
            'price': '$29.99'
        }
        
        lead = scraper._create_lead_from_product(product_data, "wireless mouse")
        
        assert lead is not None
        assert lead.company_name == "TechGear"
        assert lead.domain == "techgear.amazon"
        assert lead.ad_creatives[0].ad_id == "B09ABC123"
    
    def test_create_lead_without_brand(self):
        """Test creating lead without brand info."""
        scraper = AmazonAdsScraper()
        
        product_data = {
            'title': 'Generic Product',
            'asin': 'B09XYZ789'
        }
        
        lead = scraper._create_lead_from_product(product_data, "test")
        assert lead is None
    
    @pytest.mark.asyncio
    async def test_browser_initialization(self):
        """Test browser context initialization."""
        with patch('scraper.amazon_ads.async_playwright') as mock_playwright:
            mock_pw = AsyncMock()
            mock_browser = AsyncMock()
            mock_context = AsyncMock()
            mock_page = AsyncMock()
            
            # Setup mock chain
            mock_playwright.return_value.start.return_value = mock_pw
            mock_pw.chromium.launch.return_value = mock_browser
            mock_browser.new_context.return_value = mock_context
            mock_context.new_page.return_value = mock_page
            
            async with AmazonAdsScraper(headless=False) as scraper:
                assert scraper.browser is not None
                assert scraper.page is not None
            
            mock_pw.chromium.launch.assert_called_with(headless=False)

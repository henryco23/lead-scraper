# Lead Scraper - Multi-Source B2B Lead Generation Tool

A powerful Python-based lead scraper that identifies currently active advertisers across multiple platforms:
- Google Ads (via Ads Transparency Center)
- Meta Ads (via Meta Ad Library)
- Amazon Sponsored Listings
- Google Shopping Ads

## Features

- **Multi-Platform Support**: Scrape leads from 4 major advertising platforms
- **Intelligent Deduplication**: Automatically merges leads from different sources by domain
- **Company Enrichment**: Fetches additional company information (contact details, LinkedIn, company size)
- **Flexible CLI**: Easy-to-use command-line interface with multiple options
- **Data Persistence**: SQLite database storage with CSV export functionality
- **Respectful Scraping**: Built-in rate limiting, retry logic, and User-Agent rotation
- **Type Safety**: Full type hints and Pydantic models for data validation

## Installation

### Prerequisites

- Python 3.11 or higher
- pip package manager

### Install from source

```bash
# Clone the repository
git clone https://github.com/yourusername/lead-scraper.git
cd lead-scraper

# Create a virtual environment (recommended)
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install the package
pip install -e .

# Install Playwright browsers (required for Amazon and Shopping scrapers)
playwright install chromium
```

### Install via pip

```bash
pip install lead-scraper
playwright install chromium
```

## Configuration

### Environment Variables

Create a `.env` file in your project root:

```env
# Meta Ad Library Access Token (required for Meta Ads)
META_ACCESS_TOKEN=your_meta_access_token_here

# Clearbit API Key (optional, for enhanced company enrichment)
CLEARBIT_API_KEY=your_clearbit_api_key_here
```

### Getting API Keys

1. **Meta Access Token**: 
   - Visit [Meta for Developers](https://developers.facebook.com/)
   - Create an app and generate an access token with `ads_read` permission

2. **Clearbit API Key** (optional):
   - Sign up at [Clearbit](https://clearbit.com/)
   - Get your API key from the dashboard

## Usage

### Basic Usage

Scrape all sources with default settings:

```bash
python -m scraper.cli --source all
```

### Specific Source

Scrape only Google Ads:

```bash
python -m scraper.cli --source google --max 500 --since 2025-05-01
```

### With Search Query

Search for specific advertisers:

```bash
python -m scraper.cli --source meta --query "software" --max 100
```

### Advanced Options

```bash
python -m scraper.cli \
    --source all \
    --max 1000 \
    --since 2025-04-01 \
    --query "B2B software" \
    --no-enrich \
    --db custom_leads.db \
    --no-headless
```

### CLI Options

- `--source`: Ad platform to scrape (google|meta|amazon|shopping|all)
- `--max`: Maximum number of leads to fetch (default: 100)
- `--since`: Start date for ads in YYYY-MM-DD format (default: 30 days ago)
- `--query`: Search query for ads
- `--enrich/--no-enrich`: Enable/disable company enrichment (default: enabled)
- `--csv/--no-csv`: Export results to CSV (default: enabled)
- `--db`: SQLite database path (default: leads.db)
- `--headless/--no-headless`: Run browser scrapers in headless mode (default: headless)

### Other Commands

View database statistics:

```bash
python -m scraper.cli stats
```

Export leads to CSV:

```bash
python -m scraper.cli export output.csv --limit 100 --active-only
```

## Python API

You can also use the scraper programmatically:

```python
import asyncio
from scraper import scrape_google_ads, scrape_meta_ads, enrich_leads, LeadDatabase

async def main():
    # Scrape Google Ads
    google_leads = await scrape_google_ads(
        query="enterprise software",
        max_results=50
    )
    
    # Scrape Meta Ads
    meta_leads = await scrape_meta_ads(
        search_terms="B2B SaaS",
        limit=50
    )
    
    # Combine and enrich
    all_leads = google_leads + meta_leads
    enriched_leads = await enrich_leads(all_leads)
    
    # Save to database
    db = LeadDatabase("my_leads.db")
    for lead in enriched_leads:
        db.upsert_lead(lead)
    
    # Export to CSV
    db.export_to_csv("leads_export.csv")

asyncio.run(main())
```

## Data Schema

### Lead Object

```python
{
    "domain": "example.com",
    "company_name": "Example Corp",
    "first_seen": "2025-05-22T10:30:00",
    "last_seen": "2025-05-22T10:30:00",
    "sources": ["google_ads", "meta_ads"],
    "ad_creatives": [...],
    "company_info": {
        "website_title": "Example Corp - Enterprise Software",
        "linkedin_url": "https://linkedin.com/company/example-corp",
        "phone": "+1-555-0123",
        "email": "info@example.com",
        "company_size": "50-200 employees",
        "industry": "Software"
    },
    "total_impressions": 150000,
    "total_spend_estimate": 5000.0
}
```

## Testing

Run the test suite:

```bash
# Install dev dependencies
pip install -e ".[dev]"

# Run tests
pytest

# Run with coverage
pytest --cov=scraper --cov-report=html
```

## Best Practices

1. **Rate Limiting**: The scraper includes built-in rate limiting. Don't disable it.
2. **Respectful Scraping**: Always respect robots.txt and terms of service.
3. **API Keys**: Keep your API keys secure and never commit them to version control.
4. **Database Backups**: Regularly backup your SQLite database.
5. **Monitoring**: Monitor your scraping activities and adjust limits if needed.

## Troubleshooting

### Common Issues

1. **CAPTCHA Detection**: 
   - Use `--no-headless` to run browsers in visible mode
   - Reduce scraping speed with longer delays
   - Rotate IP addresses if possible

2. **Meta API Errors**:
   - Ensure your access token has proper permissions
   - Check rate limits on your Meta app

3. **No Results Found**:
   - Try broader search queries
   - Check if the platform has data for your date range
   - Verify API credentials

### Debug Mode

Enable debug logging:

```python
import logging
logging.basicConfig(level=logging.DEBUG)
```

## Legal Disclaimer

This tool is for educational and legitimate business purposes only. Users are responsible for:
- Complying with all applicable laws and regulations
- Respecting website terms of service and robots.txt
- Using scraped data ethically and legally
- Not overloading target servers

## Contributing

Contributions are welcome! Please:
1. Fork the repository
2. Create a feature branch
3. Add tests for new functionality
4. Ensure all tests pass
5. Submit a pull request

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Acknowledgments

- Built with Playwright for reliable browser automation
- Uses aiohttp for efficient async HTTP requests
- Powered by Typer for the CLI interface
- Data validation with Pydantic

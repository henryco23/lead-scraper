"""Command-line interface for the lead scraper."""
import asyncio
import os
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional, List
from enum import Enum

import typer
from rich.console import Console
from rich.table import Table
from rich.progress import Progress, SpinnerColumn, TextColumn
from dotenv import load_dotenv

from .models import AdSource, Lead, ScrapeResult
from .db import LeadDatabase
from .google_ads import scrape_google_ads
from .meta_ads import scrape_meta_ads
from .amazon_ads import scrape_amazon_ads
from .shopping_ads import scrape_shopping_ads
from .enrich import enrich_leads
from .utils import setup_logger

# Load environment variables
load_dotenv()

app = typer.Typer(help="Multi-source lead scraper for B2B sales teams")
console = Console()
logger = setup_logger(__name__)


class SourceChoice(str, Enum):
    """CLI source choices."""
    GOOGLE = "google"
    META = "meta"
    AMAZON = "amazon"
    SHOPPING = "shopping"
    ALL = "all"


def parse_date(date_str: str) -> datetime:
    """Parse ISO date string."""
    try:
        return datetime.fromisoformat(date_str)
    except:
        try:
            return datetime.strptime(date_str, "%Y-%m-%d")
        except:
            raise typer.BadParameter(f"Invalid date format: {date_str}. Use YYYY-MM-DD")


@app.command()
def scrape(
    source: SourceChoice = typer.Option(
        SourceChoice.ALL,
        "--source",
        "-s",
        help="Ad platform to scrape"
    ),
    max_leads: int = typer.Option(
        100,
        "--max",
        "-m",
        help="Maximum number of leads to fetch"
    ),
    since: Optional[str] = typer.Option(
        None,
        "--since",
        help="Start date for ads (ISO format: YYYY-MM-DD)"
    ),
    query: Optional[str] = typer.Option(
        None,
        "--query",
        "-q",
        help="Search query for ads"
    ),
    enrich: bool = typer.Option(
        True,
        "--enrich/--no-enrich",
        help="Enrich leads with company information"
    ),
    export_csv: bool = typer.Option(
        True,
        "--csv/--no-csv",
        help="Export results to CSV"
    ),
    db_path: str = typer.Option(
        "leads.db",
        "--db",
        help="SQLite database path"
    ),
    headless: bool = typer.Option(
        True,
        "--headless/--no-headless",
        help="Run browser scrapers in headless mode"
    )
):
    """Scrape leads from advertising platforms."""
    console.print(f"[bold green]Lead Scraper[/bold green] - Starting scrape operation")
    
    # Parse start date
    start_date = parse_date(since) if since else datetime.utcnow() - timedelta(days=30)
    
    # Initialize database
    db = LeadDatabase(db_path)
    
    # Determine which sources to scrape
    sources_to_scrape = []
    if source == SourceChoice.ALL:
        sources_to_scrape = [AdSource.GOOGLE_ADS, AdSource.META_ADS, 
                            AdSource.AMAZON_ADS, AdSource.SHOPPING_ADS]
    else:
        source_map = {
            SourceChoice.GOOGLE: AdSource.GOOGLE_ADS,
            SourceChoice.META: AdSource.META_ADS,
            SourceChoice.AMAZON: AdSource.AMAZON_ADS,
            SourceChoice.SHOPPING: AdSource.SHOPPING_ADS
        }
        sources_to_scrape = [source_map[source]]
    
    # Run scrapers
    all_leads = []
    results = []
    
    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        console=console
    ) as progress:
        for ad_source in sources_to_scrape:
            task = progress.add_task(f"Scraping {ad_source.value}...", total=None)
            
            try:
                start_time = datetime.utcnow()
                leads = asyncio.run(scrape_source(
                    ad_source, 
                    query=query,
                    start_date=start_date,
                    max_results=max_leads,
                    headless=headless
                ))
                
                duration = (datetime.utcnow() - start_time).total_seconds()
                
                result = ScrapeResult(
                    source=ad_source,
                    success=True,
                    leads_found=len(leads),
                    duration_seconds=duration
                )
                results.append(result)
                all_leads.extend(leads)
                
                progress.update(task, description=f"✓ {ad_source.value}: {len(leads)} leads")
                
            except Exception as e:
                logger.error(f"Error scraping {ad_source.value}: {e}")
                result = ScrapeResult(
                    source=ad_source,
                    success=False,
                    errors=[str(e)]
                )
                results.append(result)
                progress.update(task, description=f"✗ {ad_source.value}: Failed")
    
    # Deduplicate leads
    console.print(f"\nDeduplicating {len(all_leads)} leads...")
    unique_leads = deduplicate_leads(all_leads)
    console.print(f"Found {len(unique_leads)} unique leads")
    
    # Enrich leads if requested
    if enrich and unique_leads:
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            console=console
        ) as progress:
            task = progress.add_task("Enriching leads...", total=None)
            enriched_leads = asyncio.run(enrich_leads(unique_leads))
            progress.update(task, description=f"✓ Enriched {len(enriched_leads)} leads")
    else:
        enriched_leads = unique_leads
    
    # Save to database
    console.print("\nSaving to database...")
    for lead in enriched_leads:
        db.upsert_lead(lead)
    
    # Export to CSV if requested
    if export_csv and enriched_leads:
        csv_filename = f"leads_{datetime.utcnow().strftime('%Y-%m-%d')}.csv"
        db.export_to_csv(csv_filename, enriched_leads)
        console.print(f"✓ Exported to {csv_filename}")
    
    # Display summary
    display_summary(results, enriched_leads, db)


async def scrape_source(
    source: AdSource,
    query: Optional[str] = None,
    start_date: Optional[datetime] = None,
    max_results: int = 100,
    headless: bool = True
) -> List[Lead]:
    """Scrape a single source."""
    if source == AdSource.GOOGLE_ADS:
        return await scrape_google_ads(
            query=query or "",
            start_date=start_date,
            max_results=max_results
        )
    
    elif source == AdSource.META_ADS:
        return await scrape_meta_ads(
            search_terms=query or "",
            limit=max_results
        )
    
    elif source == AdSource.AMAZON_ADS:
        # Default queries if none provided
        queries = [query] if query else [
            "electronics", "home improvement", "health supplements",
            "office supplies", "software tools"
        ]
        return await scrape_amazon_ads(
            search_queries=queries,
            max_results_per_query=max_results // len(queries),
            headless=headless
        )
    
    elif source == AdSource.SHOPPING_ADS:
        # Default queries if none provided
        queries = [query] if query else [
            "business software", "office furniture", "industrial equipment",
            "wholesale supplies", "professional services"
        ]
        return await scrape_shopping_ads(
            search_queries=queries,
            max_results_per_query=max_results // len(queries),
            headless=headless
        )
    
    return []


def deduplicate_leads(leads: List[Lead]) -> List[Lead]:
    """Deduplicate leads by domain."""
    unique_leads = {}
    
    for lead in leads:
        if lead.domain in unique_leads:
            # Merge with existing lead
            unique_leads[lead.domain].merge_with(lead)
        else:
            unique_leads[lead.domain] = lead
    
    return list(unique_leads.values())


def display_summary(results: List[ScrapeResult], leads: List[Lead], db: LeadDatabase):
    """Display scraping summary."""
    console.print("\n[bold]Scraping Summary[/bold]")
    
    # Results table
    table = Table(show_header=True, header_style="bold magenta")
    table.add_column("Source", style="cyan")
    table.add_column("Status", style="green")
    table.add_column("Leads Found", justify="right")
    table.add_column("Duration (s)", justify="right")
    
    for result in results:
        status = "✓ Success" if result.success else "✗ Failed"
        status_style = "green" if result.success else "red"
        table.add_row(
            result.source.value,
            f"[{status_style}]{status}[/{status_style}]",
            str(result.leads_found),
            f"{result.duration_seconds:.1f}"
        )
    
    console.print(table)
    
    # Database stats
    stats = db.get_stats()
    console.print(f"\n[bold]Database Statistics[/bold]")
    console.print(f"Total leads: {stats['total_leads']}")
    console.print(f"Active leads: {stats['active_leads']}")
    console.print(f"Total ad creatives: {stats['total_creatives']}")
    
    # Sample leads
    if leads:
        console.print(f"\n[bold]Sample Leads (first 5)[/bold]")
        sample_table = Table(show_header=True, header_style="bold blue")
        sample_table.add_column("Company", style="cyan")
        sample_table.add_column("Domain")
        sample_table.add_column("Sources")
        sample_table.add_column("Enriched", justify="center")
        
        for lead in leads[:5]:
            sources = ", ".join(s.value for s in lead.sources)
            enriched = "✓" if lead.company_info else "✗"
            sample_table.add_row(
                lead.company_name,
                lead.domain,
                sources,
                enriched
            )
        
        console.print(sample_table)


@app.command()
def stats(
    db_path: str = typer.Option("leads.db", "--db", help="SQLite database path")
):
    """Display database statistics."""
    db = LeadDatabase(db_path)
    stats = db.get_stats()
    
    console.print("[bold]Lead Database Statistics[/bold]\n")
    console.print(f"Total leads: {stats['total_leads']}")
    console.print(f"Active leads: {stats['active_leads']}")
    console.print(f"Total ad creatives: {stats['total_creatives']}")
    
    if stats.get('leads_by_source'):
        console.print("\n[bold]Leads by Source:[/bold]")
        for source, count in stats['leads_by_source'].items():
            console.print(f"  {source}: {count}")


@app.command()
def export(
    output: str = typer.Argument("leads.csv", help="Output CSV filename"),
    db_path: str = typer.Option("leads.db", "--db", help="SQLite database path"),
    active_only: bool = typer.Option(True, "--active-only/--all", help="Export only active leads"),
    limit: Optional[int] = typer.Option(None, "--limit", "-l", help="Limit number of leads")
):
    """Export leads to CSV."""
    db = LeadDatabase(db_path)
    leads = db.get_all_leads(active_only=active_only, limit=limit)
    
    if not leads:
        console.print("[yellow]No leads found to export[/yellow]")
        return
    
    db.export_to_csv(output, leads)
    console.print(f"[green]✓ Exported {len(leads)} leads to {output}[/green]")


def main():
    """Main entry point."""
    app()


if __name__ == "__main__":
    main()

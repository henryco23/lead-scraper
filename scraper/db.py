"""Database operations for lead storage and retrieval."""
import sqlite3
from typing import List, Optional, Dict, Any
from datetime import datetime
import json
from pathlib import Path

from .models import Lead, AdCreative, CompanyInfo, AdSource
from .utils import setup_logger

logger = setup_logger(__name__)


class LeadDatabase:
    """Handle SQLite database operations for leads."""
    
    def __init__(self, db_path: str = "leads.db"):
        self.db_path = db_path
        self._init_db()
    
    def _init_db(self) -> None:
        """Initialize database with required tables."""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            
            # Create leads table
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS leads (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    domain TEXT UNIQUE NOT NULL,
                    company_name TEXT NOT NULL,
                    first_seen TIMESTAMP NOT NULL,
                    last_seen TIMESTAMP NOT NULL,
                    sources TEXT NOT NULL,  -- JSON array
                    total_impressions INTEGER,
                    total_spend_estimate REAL,
                    is_active BOOLEAN DEFAULT 1,
                    company_info TEXT,  -- JSON object
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            # Create ad_creatives table
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS ad_creatives (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    lead_id INTEGER NOT NULL,
                    ad_id TEXT,
                    advertiser_name TEXT NOT NULL,
                    creative_url TEXT,
                    campaign_start_date TIMESTAMP,
                    impressions INTEGER,
                    spend_estimate REAL,
                    landing_page_url TEXT,
                    source TEXT NOT NULL,
                    scraped_at TIMESTAMP NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (lead_id) REFERENCES leads (id),
                    UNIQUE(ad_id, source)
                )
            ''')
            
            # Create indexes
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_leads_domain ON leads (domain)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_leads_active ON leads (is_active)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_creatives_lead ON ad_creatives (lead_id)')
            
            conn.commit()
            logger.info(f"Database initialized at {self.db_path}")
    
    def upsert_lead(self, lead: Lead) -> int:
        """Insert or update a lead record. Returns lead ID."""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            
            # Check if lead exists
            cursor.execute('SELECT id FROM leads WHERE domain = ?', (lead.domain,))
            existing = cursor.fetchone()
            
            if existing:
                lead_id = existing[0]
                # Update existing lead
                cursor.execute('''
                    UPDATE leads 
                    SET company_name = ?,
                        last_seen = ?,
                        sources = ?,
                        total_impressions = ?,
                        total_spend_estimate = ?,
                        is_active = ?,
                        company_info = ?,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE id = ?
                ''', (
                    lead.company_name,
                    lead.last_seen.isoformat(),
                    json.dumps([s.value for s in lead.sources]),
                    lead.total_impressions,
                    lead.total_spend_estimate,
                    lead.is_active,
                    json.dumps(lead.company_info.dict()) if lead.company_info else None,
                    lead_id
                ))
                logger.debug(f"Updated lead {lead.domain} (ID: {lead_id})")
            else:
                # Insert new lead
                cursor.execute('''
                    INSERT INTO leads (
                        domain, company_name, first_seen, last_seen, sources,
                        total_impressions, total_spend_estimate, is_active, company_info
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    lead.domain,
                    lead.company_name,
                    lead.first_seen.isoformat(),
                    lead.last_seen.isoformat(),
                    json.dumps([s.value for s in lead.sources]),
                    lead.total_impressions,
                    lead.total_spend_estimate,
                    lead.is_active,
                    json.dumps(lead.company_info.dict()) if lead.company_info else None
                ))
                lead_id = cursor.lastrowid
                logger.debug(f"Inserted new lead {lead.domain} (ID: {lead_id})")
            
            # Insert ad creatives
            for creative in lead.ad_creatives:
                self._insert_creative(cursor, lead_id, creative)
            
            conn.commit()
            return lead_id
    
    def _insert_creative(self, cursor: sqlite3.Cursor, lead_id: int, creative: AdCreative) -> None:
        """Insert an ad creative if it doesn't exist."""
        try:
            cursor.execute('''
                INSERT OR IGNORE INTO ad_creatives (
                    lead_id, ad_id, advertiser_name, creative_url,
                    campaign_start_date, impressions, spend_estimate,
                    landing_page_url, source, scraped_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                lead_id,
                creative.ad_id,
                creative.advertiser_name,
                str(creative.creative_url) if creative.creative_url else None,
                creative.campaign_start_date.isoformat() if creative.campaign_start_date else None,
                creative.impressions,
                creative.spend_estimate,
                str(creative.landing_page_url) if creative.landing_page_url else None,
                creative.source.value,
                creative.scraped_at.isoformat()
            ))
        except sqlite3.IntegrityError:
            # Creative already exists
            pass
    
    def get_lead_by_domain(self, domain: str) -> Optional[Lead]:
        """Retrieve a lead by domain."""
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            
            cursor.execute('SELECT * FROM leads WHERE domain = ?', (domain,))
            row = cursor.fetchone()
            
            if not row:
                return None
            
            # Get ad creatives
            cursor.execute(
                'SELECT * FROM ad_creatives WHERE lead_id = ?',
                (row['id'],)
            )
            creative_rows = cursor.fetchall()
            
            # Reconstruct lead object
            lead = Lead(
                domain=row['domain'],
                company_name=row['company_name'],
                first_seen=datetime.fromisoformat(row['first_seen']),
                last_seen=datetime.fromisoformat(row['last_seen']),
                sources=[AdSource(s) for s in json.loads(row['sources'])],
                total_impressions=row['total_impressions'],
                total_spend_estimate=row['total_spend_estimate'],
                is_active=bool(row['is_active'])
            )
            
            # Add company info if available
            if row['company_info']:
                lead.company_info = CompanyInfo(**json.loads(row['company_info']))
            
            # Add ad creatives
            for creative_row in creative_rows:
                creative = AdCreative(
                    ad_id=creative_row['ad_id'],
                    advertiser_name=creative_row['advertiser_name'],
                    creative_url=creative_row['creative_url'],
                    campaign_start_date=datetime.fromisoformat(creative_row['campaign_start_date']) 
                        if creative_row['campaign_start_date'] else None,
                    impressions=creative_row['impressions'],
                    spend_estimate=creative_row['spend_estimate'],
                    landing_page_url=creative_row['landing_page_url'],
                    source=AdSource(creative_row['source']),
                    scraped_at=datetime.fromisoformat(creative_row['scraped_at'])
                )
                lead.ad_creatives.append(creative)
            
            return lead
    
    def get_all_leads(
        self,
        active_only: bool = True,
        since: Optional[datetime] = None,
        limit: Optional[int] = None
    ) -> List[Lead]:
        """Retrieve all leads matching criteria."""
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            
            query = 'SELECT * FROM leads WHERE 1=1'
            params: List[Any] = []
            
            if active_only:
                query += ' AND is_active = 1'
            
            if since:
                query += ' AND last_seen >= ?'
                params.append(since.isoformat())
            
            query += ' ORDER BY last_seen DESC'
            
            if limit:
                query += ' LIMIT ?'
                params.append(limit)
            
            cursor.execute(query, params)
            rows = cursor.fetchall()
            
            leads = []
            for row in rows:
                lead = self.get_lead_by_domain(row['domain'])
                if lead:
                    leads.append(lead)
            
            return leads
    
    def export_to_csv(self, output_path: str, leads: Optional[List[Lead]] = None) -> None:
        """Export leads to CSV file."""
        import csv
        
        if leads is None:
            leads = self.get_all_leads()
        
        fieldnames = [
            'domain', 'company_name', 'first_seen', 'last_seen',
            'sources', 'total_impressions', 'total_spend_estimate',
            'website_title', 'linkedin_url', 'phone', 'email',
            'company_size', 'industry', 'num_creatives', 'is_active'
        ]
        
        with open(output_path, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            
            for lead in leads:
                row = {
                    'domain': lead.domain,
                    'company_name': lead.company_name,
                    'first_seen': lead.first_seen.isoformat(),
                    'last_seen': lead.last_seen.isoformat(),
                    'sources': ', '.join(s.value for s in lead.sources),
                    'total_impressions': lead.total_impressions,
                    'total_spend_estimate': lead.total_spend_estimate,
                    'num_creatives': len(lead.ad_creatives),
                    'is_active': lead.is_active
                }
                
                # Add company info fields
                if lead.company_info:
                    row.update({
                        'website_title': lead.company_info.website_title,
                        'linkedin_url': str(lead.company_info.linkedin_url) if lead.company_info.linkedin_url else None,
                        'phone': lead.company_info.phone,
                        'email': lead.company_info.email,
                        'company_size': lead.company_info.company_size,
                        'industry': lead.company_info.industry
                    })
                
                writer.writerow(row)
        
        logger.info(f"Exported {len(leads)} leads to {output_path}")
    
    def get_stats(self) -> Dict[str, Any]:
        """Get database statistics."""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            
            stats = {}
            
            # Total leads
            cursor.execute('SELECT COUNT(*) FROM leads')
            stats['total_leads'] = cursor.fetchone()[0]
            
            # Active leads
            cursor.execute('SELECT COUNT(*) FROM leads WHERE is_active = 1')
            stats['active_leads'] = cursor.fetchone()[0]
            
            # Total ad creatives
            cursor.execute('SELECT COUNT(*) FROM ad_creatives')
            stats['total_creatives'] = cursor.fetchone()[0]
            
            # Leads by source
            cursor.execute('''
                SELECT sources, COUNT(*) as count 
                FROM leads 
                GROUP BY sources
            ''')
            source_counts = {}
            for row in cursor.fetchall():
                sources = json.loads(row[0])
                for source in sources:
                    source_counts[source] = source_counts.get(source, 0) + row[1]
            stats['leads_by_source'] = source_counts
            
            return stats

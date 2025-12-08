from typing import Dict, List, Optional
from datetime import datetime
from utils.db_adapter import get_db

class EnrichmentPipeline:
    """
    Enrichment waterfall for external data sources
    
    Priority order (cheapest → most accurate):
    1. Wiza (email enrichment)
    2. Apollo (person + company data)
    3. Clay (multi-source waterfall)
    4. Custom scrapers (LinkedIn, practice websites)
    
    Tracks provenance for scoring and cost attribution
    """
    
    def __init__(self):
        self.providers = {
            'wiza': self._enrich_wiza,
            'apollo': self._enrich_apollo,
            'clay': self._enrich_clay,
        }
    
    def _enrich_wiza(self, provider_id: str, data: Dict) -> Optional[Dict]:
        """Enrich using Wiza API"""
        # TODO: Implement Wiza API call
        print(f"📧 Wiza enrichment for {provider_id}")
        return None
    
    def _enrich_apollo(self, provider_id: str, data: Dict) -> Optional[Dict]:
        """Enrich using Apollo API"""
        # TODO: Implement Apollo API call
        print(f"🌐 Apollo enrichment for {provider_id}")
        return None
    
    def _enrich_clay(self, provider_id: str, data: Dict) -> Optional[Dict]:
        """Enrich using Clay waterfall"""
        # TODO: Implement Clay API call
        print(f"🎯 Clay enrichment for {provider_id}")
        return None
    
    def run(self, batch_size: int = 100):
        """
        Run enrichment waterfall on un-enriched records
        
        Process:
        1. Fetch records from CLEAN layer
        2. Try each enrichment provider in priority order
        3. Stop at first successful enrichment
        4. Write results to ENRICHED.PROVIDERS_MASTER
        """
        print(f"🚀 Starting enrichment pipeline at {datetime.now()}")
        
        with get_db() as db:
            # Fetch un-enriched records
            fetch_sql = f"""
            SELECT provider_id, npi, practice_name, city, state
            FROM CLEAN.PROVIDERS_VALIDATED
            WHERE provider_id NOT IN (SELECT provider_id FROM ENRICHED.PROVIDERS_MASTER)
            LIMIT {batch_size}
            """
            
            records = db.execute(fetch_sql)
            
            if not records:
                print("✅ No records to enrich")
                return
            
            print(f"📋 Enriching {len(records)} records")
            
            enriched_count = 0
            for record in records:
                provider_id = record[0]
                data = dict(zip(['provider_id', 'npi', 'practice_name', 'city', 'state'], record))
                
                # Try each provider in waterfall order
                enriched_data = None
                provider_used = None
                
                for provider_name, enrich_func in self.providers.items():
                    enriched_data = enrich_func(provider_id, data)
                    if enriched_data:
                        provider_used = provider_name
                        enriched_count += 1
                        break
                
                if enriched_data:
                    print(f"✅ Enriched {provider_id} using {provider_used}")
            
            print(f"✅ Enrichment complete: {enriched_count}/{len(records)} records enriched")

def run():
    pipeline = EnrichmentPipeline()
    pipeline.run()

if __name__ == "__main__":
    run()

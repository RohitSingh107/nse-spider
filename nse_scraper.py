import requests
import time
import json
from datetime import datetime
import random
import brotli
import zlib
import base64
import sys
import yaml

class NSEScraper:
    def __init__(self):
        self.base_url = "https://www.nseindia.com"
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:122.0) Gecko/20100101 Firefox/122.0',
            'Accept': 'application/json, text/plain, */*',
            'Accept-Language': 'en-US,en;q=0.9',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
            'Referer': 'https://www.nseindia.com/get-quotes/equity',
            'Sec-Fetch-Dest': 'empty',
            'Sec-Fetch-Mode': 'cors',
            'Sec-Fetch-Site': 'same-origin',
            'Cache-Control': 'no-cache',
            'Pragma': 'no-cache',
            'X-Requested-With': 'XMLHttpRequest'
        }
        self.session = requests.Session()
        self.get_cookies()

    def get_cookies(self):
        """Get cookies from NSE website"""
        try:
            response = self.session.get(self.base_url, headers=self.headers)
            if response.status_code == 200:
                quotes_url = f"{self.base_url}/get-quotes/equity"
                response = self.session.get(quotes_url, headers=self.headers)
                if response.status_code == 200:
                    return True
                return False
            return False
        except Exception:
            return False

    def decompress_response(self, response):
        """Decompress the response content"""
        try:
            content = response.content
            content_encoding = response.headers.get('Content-Encoding', '').lower()
            
            if 'br' in content_encoding:
                try:
                    decompressed = brotli.decompress(content)
                    return decompressed.decode('utf-8')
                except Exception:
                    pass
            
            elif 'gzip' in content_encoding:
                try:
                    decompressed = zlib.decompress(content, 16 + zlib.MAX_WBITS)
                    return decompressed.decode('utf-8')
                except Exception:
                    try:
                        decompressed = zlib.decompress(content)
                        return decompressed.decode('utf-8')
                    except Exception:
                        pass
            
            try:
                return content.decode('utf-8')
            except Exception:
                return None
                
        except Exception:
            return None

    def get_quote_data(self, symbol):
        """Get quote data for a specific symbol"""
        try:
            url = f"{self.base_url}/api/quote-equity?symbol={symbol}"
            time.sleep(random.uniform(1, 2))
            
            api_headers = self.headers.copy()
            api_headers.update({
                'Referer': f'https://www.nseindia.com/get-quotes/equity?symbol={symbol}',
                'Origin': 'https://www.nseindia.com'
            })
            
            response = self.session.get(url, headers=api_headers)
            
            if response.status_code == 200:
                decompressed_content = self.decompress_response(response)
                if decompressed_content:
                    try:
                        data = json.loads(decompressed_content)
                        if isinstance(data, dict) and 'error' in data:
                            return None
                        return {
                            "symbol": symbol,
                            "timestamp": datetime.now().isoformat(),
                            "data": data
                        }
                    except json.JSONDecodeError:
                        return None
                return None
            return None
            
        except Exception:
            return None

def scrape_symbols(symbols, output_file=None):
    """Process a list of symbols and optionally save results to a file"""
    scraper = NSEScraper()
    results = []
    
    for i, symbol in enumerate(symbols, 1):
        try:
            data = scraper.get_quote_data(symbol)
            if data:
                results.append(data)
        except Exception:
            continue
        
        if i < len(symbols):
            time.sleep(random.uniform(2, 4))
    
    if output_file and results:
        try:
            with open(output_file, 'w') as f:
                json.dump(results, f, indent=2)
        except Exception:
            pass
    
    return results

def main():
    try:
        with open('input.yaml', 'r') as f:
            config = yaml.safe_load(f)
            symbols = list(config['tickers'].keys())
            output_file = "nse_quotes.json"
    except Exception:
        return
    
    results = scrape_symbols(symbols, output_file)

if __name__ == "__main__":
    main() 

import requests
import time
import json
from datetime import datetime
import random
import brotli
import zlib
import base64
import sys

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
            print("Getting cookies...")
            # First visit the main page
            response = self.session.get(self.base_url, headers=self.headers)
            if response.status_code == 200:
                # Then visit the quotes page to get additional cookies
                quotes_url = f"{self.base_url}/get-quotes/equity"
                response = self.session.get(quotes_url, headers=self.headers)
                if response.status_code == 200:
                    print("Successfully obtained cookies")
                    print("Cookies obtained:", dict(self.session.cookies))
                    return True
                else:
                    print(f"Failed to get quotes page cookies. Status code: {response.status_code}")
                    return False
            else:
                print(f"Failed to get main page cookies. Status code: {response.status_code}")
                return False
        except Exception as e:
            print(f"Error getting cookies: {e}")
            return False

    def decompress_response(self, response):
        """Decompress the response content"""
        try:
            content = response.content
            content_encoding = response.headers.get('Content-Encoding', '').lower()
            content_type = response.headers.get('Content-Type', '')
            
            print(f"Content length: {len(content)}")
            print(f"Content encoding: {content_encoding}")
            print(f"Content type: {content_type}")
            
            # Save raw content for inspection
            with open('raw_response.bin', 'wb') as f:
                f.write(content)
            print("Saved raw response to raw_response.bin")
            
            # Try different decompression methods
            if 'br' in content_encoding:
                try:
                    # Try Brotli decompression
                    decompressed = brotli.decompress(content)
                    return decompressed.decode('utf-8')
                except Exception as e:
                    print(f"Brotli decompression failed: {e}")
            
            elif 'gzip' in content_encoding:
                try:
                    # Try different gzip decompression approaches
                    try:
                        decompressed = zlib.decompress(content, 16 + zlib.MAX_WBITS)
                        return decompressed.decode('utf-8')
                    except Exception as e:
                        print(f"Standard gzip decompression failed: {e}")
                        
                    try:
                        # Try without the gzip header
                        decompressed = zlib.decompress(content)
                        return decompressed.decode('utf-8')
                    except Exception as e:
                        print(f"Raw zlib decompression failed: {e}")
                except Exception as e:
                    print(f"All gzip attempts failed: {e}")
            
            # If no compression specified or all decompression attempts failed
            try:
                return content.decode('utf-8')
            except Exception as e:
                print(f"Final raw decode attempt failed: {e}")
                return None
                
        except Exception as e:
            print(f"Decompression error: {e}")
            return None

    def get_quote_data(self, symbol):
        """Get quote data for a specific symbol"""
        try:
            print(f"Fetching data for symbol: {symbol}")
            url = f"{self.base_url}/api/quote-equity?symbol={symbol}"
            print(f"Request URL: {url}")
            
            # Add random delay to avoid rate limiting
            time.sleep(random.uniform(1, 2))
            
            # Update headers for the API request
            api_headers = self.headers.copy()
            api_headers.update({
                'Referer': f'https://www.nseindia.com/get-quotes/equity?symbol={symbol}',
                'Origin': 'https://www.nseindia.com'
            })
            
            print("Request headers:", json.dumps(api_headers, indent=2))
            print("Current cookies:", dict(self.session.cookies))
            
            response = self.session.get(url, headers=api_headers)
            
            print(f"Response status code: {response.status_code}")
            print(f"Response headers: {dict(response.headers)}")
            
            if response.status_code == 200:
                try:
                    # Decompress and parse the response
                    decompressed_content = self.decompress_response(response)
                    if decompressed_content:
                        try:
                            data = json.loads(decompressed_content)
                            # Check if the response indicates an error
                            if isinstance(data, dict) and 'error' in data:
                                print(f"API returned error for {symbol}: {data['error']}")
                                return None
                            return {
                                "symbol": symbol,
                                "timestamp": datetime.now().isoformat(),
                                "data": data
                            }
                        except json.JSONDecodeError as e:
                            print(f"JSON parsing failed: {e}")
                            print("First 500 characters of decompressed content:")
                            print(decompressed_content[:500])
                            return None
                    else:
                        print("Failed to decompress response")
                        return None
                except Exception as e:
                    print(f"Failed to parse response: {e}")
                    print(f"Raw response content type: {response.headers.get('Content-Type')}")
                    print(f"Content encoding: {response.headers.get('Content-Encoding')}")
                    return None
            else:
                print(f"Failed to get quote data. Status code: {response.status_code}")
                print(f"Response: {response.text}")
                print(f"Cookies: {dict(self.session.cookies)}")
                return None
            
        except Exception as e:
            print(f"Error getting quote data: {e}")
            return None

def process_symbols(symbols, output_file=None):
    """Process a list of symbols and optionally save results to a file"""
    scraper = NSEScraper()
    results = []
    
    for i, symbol in enumerate(symbols, 1):
        print(f"\nProcessing symbol {i}/{len(symbols)}: {symbol}")
        try:
            data = scraper.get_quote_data(symbol)
            if data:
                results.append(data)
                print(f"Successfully retrieved data for {symbol}")
            else:
                print(f"Failed to retrieve data for {symbol}")
        except Exception as e:
            print(f"Error processing {symbol}: {e}")
        
        # Add a delay between symbols to avoid rate limiting
        if i < len(symbols):
            delay = random.uniform(2, 4)
            print(f"Waiting {delay:.1f} seconds before next request...")
            time.sleep(delay)
    
    # Save results if output file is specified
    if output_file and results:
        try:
            with open(output_file, 'w') as f:
                json.dump(results, f, indent=2)
            print(f"\nResults saved to {output_file}")
        except Exception as e:
            print(f"Error saving results to file: {e}")
    
    return results

def main():
    # Check if symbols are provided as command line arguments
    if len(sys.argv) > 1:
        symbols = sys.argv[1:]
        output_file = "nse_quotes.json"
    else:
        # Default symbols if none provided
        symbols = ["MON100", "SMALLCAP", "ALPHA"]
        output_file = "nse_quotes.json"
    
    print(f"Processing symbols: {', '.join(symbols)}")
    results = process_symbols(symbols, output_file)
    
    # Print summary
    print(f"\nProcessing complete. Successfully retrieved data for {len(results)}/{len(symbols)} symbols.")
    if results:
        print(f"Results have been saved to {output_file}")

if __name__ == "__main__":
    main() 
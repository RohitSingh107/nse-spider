import yaml
import math
from datetime import date
import sys
from nse_scraper import scrape_symbols

def process_rebalancing():
    try:
        with open("input.yaml", "r") as file:
            data = yaml.safe_load(file)

        # Get prices using the existing scrape_symbols function
        results = scrape_symbols(list(data['tickers'].keys()))
        
        # Extract price information
        etfs_prices_info = {}
        for result in results:
            if result and 'data' in result and 'priceInfo' in result['data']:
                symbol = result['symbol']
                price_info = result['data']['priceInfo']
                etfs_prices_info[symbol] = {
                    'price': price_info['lastPrice'],
                    'change': price_info['pChange']
                }

        per_ticker = data['dail_limit'] / len(data['tickers'].keys())
        total_dip = 0
        today_amount_allowed = 0
        buys = {}
        total_expenditure = 0

        # Calculate total dip and today's allowed amount
        for t in data['tickers'].keys():
            if t in etfs_prices_info and etfs_prices_info[t]['change'] < -0.1:
                total_dip += abs(data['tickers'][t]['weight'] * data['tickers'][t]['rebalance_factor'] * etfs_prices_info[t]['change'])
                today_amount_allowed += (data['tickers'][t]['weight'] * per_ticker)

        # Calculate buys for each ticker
        for t in data['tickers'].keys():
            if t in etfs_prices_info and etfs_prices_info[t]['change'] < data['threshold']:
                dip = abs(data['tickers'][t]['weight'] * data['tickers'][t]['rebalance_factor'] * etfs_prices_info[t]['change'])
                alloted_amount = today_amount_allowed * dip / total_dip
                ideal_no = alloted_amount / etfs_prices_info[t]['price']

                if alloted_amount - (math.floor(ideal_no) * etfs_prices_info[t]['price']) < (math.ceil(ideal_no) * etfs_prices_info[t]['price']) - alloted_amount:
                    num_shares = math.floor(ideal_no)
                else:
                    num_shares = math.ceil(ideal_no)

                # Only add to buys if number of shares is greater than 0
                if num_shares > 0:
                    buys[t] = num_shares
                    total_expenditure += num_shares * etfs_prices_info[t]['price']

        # Update weights if rebalance flag is present
        if "rw" in sys.argv:
            for t in data['tickers'].keys():
                if t in buys:  # Only adjust weights for tickers that have buy orders
                    data['tickers'][t]['weight'] = 1
                else:
                    data['tickers'][t]['weight'] += 1

            data['last_updated'] = date.today()

            with open('input.yaml', 'w') as f:
                yaml.dump(data, f)

        return buys, total_expenditure

    except Exception as e:
        print(f"Error in rebalancing process: {e}")
        return None, None

if __name__ == "__main__":
    buys, total_expenditure = process_rebalancing()
    if buys and total_expenditure is not None:
        print("Buy orders:", buys)
        print("Total Expenditure:", total_expenditure) 
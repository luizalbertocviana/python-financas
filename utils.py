import concurrent.futures
from concurrent.futures import ThreadPoolExecutor

from yahooquery import Ticker
import pandas as pd

def yahoo_symbol(name: str) -> str:
    return name.upper() + '.SA'

def how_close_to_low(stock):
    high = float(stock['fiftyTwoWeekHigh'])
    low = float(stock['fiftyTwoWeekLow'])
    price = float(stock['previousClose'])

    length = high - low
    
    return (price - low) / length

def beta_stable_or_fast_growing(stock):
    beta = stock['beta']
    
    return abs(1.0 - beta)

def create_rank_dataframe(codes) -> pd.DataFrame :
    with ThreadPoolExecutor() as executor:
        ticker_future = {code : executor.submit(Ticker, yahoo_symbol(code)) for code in codes}
        ticker = {code : ticker_future[code].result() for code in codes}

    dict_summary_detail = {yahoo_symbol(code) : ticker[code].summary_detail[yahoo_symbol(code)] 
                           for code in ticker.keys()}
    dict_financial_data = {yahoo_symbol(code) : ticker[code].financial_data[yahoo_symbol(code)] 
                           for code in ticker.keys()}
    dict_key_stats      = {yahoo_symbol(code) : ticker[code].key_stats[yahoo_symbol(code)]
                           for code in ticker.keys()}

    dataframe_financial_data = pd.DataFrame(dict_financial_data).transpose()
    dataframe_summary_detail = pd.DataFrame(dict_summary_detail).transpose()
    dataframe_key_stats      = pd.DataFrame(dict_key_stats).transpose()

    parameters_summary_details = [
        'previousClose',
        'fiftyTwoWeekLow',
        'fiftyTwoWeekHigh',
        'fiveYearAvgDividendYield',
        'payoutRatio',
        'beta',
        'trailingPE',
        'priceToSalesTrailing12Months'
    ]
    parameters_financial_data = [
        'quickRatio',
        'currentRatio',
        'returnOnAssets',
        'returnOnEquity',
        'debtToEquity',
        'grossProfits',
        'earningsGrowth'
    ]
    parameters_key_stats = [
        'floatShares',
        'priceToBook',
        'enterpriseToRevenue',
        'enterpriseToEbitda',
        'bookValue'
    ]
    view_dataframe_summary_detail = dataframe_summary_detail[parameters_summary_details]
    view_dataframe_financial_data = dataframe_financial_data[parameters_financial_data]
    view_dataframe_key_stats      =  dataframe_key_stats[parameters_key_stats]

    data = pd.concat([view_dataframe_financial_data,
                      view_dataframe_key_stats,
                      view_dataframe_summary_detail],
                     axis='columns')

    criteria = pd.DataFrame(index=data.index)

    criteria['CTL'] = data.apply(how_close_to_low, axis='columns')
    criteria['SOFG'] = data.apply(beta_stable_or_fast_growing, axis='columns')
    criteria['QR'] = data['quickRatio']
    criteria['CR'] = data['currentRatio']
    criteria['ROA'] = data['returnOnAssets']
    criteria['ROE'] = data['returnOnEquity']
    criteria['P/B'] = data['priceToBook']
    criteria['P/E'] = data['trailingPE']
    criteria['P/S'] = data['priceToSalesTrailing12Months']
    criteria['DY'] = data['fiveYearAvgDividendYield']
    criteria['DPR'] = data['payoutRatio']

    rank = pd.DataFrame(index=data.index)

    rank['CTL'] = criteria['CTL'].rank(na_option='bottom')
    rank['SOFG'] = criteria['SOFG'].rank(na_option='bottom', ascending=False)
    rank['QR'] = criteria['QR'].rank(na_option='bottom', ascending=False)
    rank['CR'] = criteria['CR'].rank(na_option='bottom', ascending=False)
    rank['ROA'] = criteria['ROA'].rank(na_option='bottom', ascending=False)
    rank['ROE'] = criteria['ROE'].rank(na_option='bottom', ascending=False)
    rank['P/B'] = criteria['P/B'].rank(na_option='bottom')
    rank['P/E'] = criteria['P/E'].rank(na_option='bottom')
    rank['P/S'] = criteria['P/S'].rank(na_option='bottom')
    rank['DY'] = criteria['DY'].rank(na_option='bottom', ascending=False)
    rank['DPR'] = criteria['DPR'].rank(na_option='bottom')

    rank['sum'] = rank.sum(axis='columns')

    return rank.sort_values(by='sum')

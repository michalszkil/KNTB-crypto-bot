# Zmiany:
# Szersze granice gridu, rzadsze transakcje (ale to do przetestowania)
# Uwzględnianie kosztów transakcyjnych - oddzielna zmienna (testnet ma zerowe fees)
# https://dev.binance.vision/t/transaction-fees-simulation-is-included-in-testnet-api/10427
# Lokalne zapisywanie wartości portfela (zmiany śledzone równolegle z tym testnetu)
# Przy transakcjach zmieniających wartość portfela, zapisywanie zmian wartości portfela
# Uwzględnienie zmian sentymentu w czasie i jego wpływu na transakcje (wpływ to też parametr do wyregulowania)
# Zapisywanie tracebacków z ewentualnymi błędami do pliku

from multiprocessing.connection import wait
from binance.client import Client
import json
from yaml import dump
from keys import api_key, private_key
import pandas as pd
import time
from time import sleep
from sqlalchemy import create_engine, MetaData
from sqlalchemy.orm import sessionmaker, scoped_session
from psaw import PushshiftAPI
from datetime import datetime, timedelta
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
import pickle
import traceback
import os

# os.chdir('C:\\Users\\Michal\\Desktop\\Szkola\\Boty\\KNTB-crypto-bot\\bot_btc_usdt')

# Inicjalizacja API
api = PushshiftAPI()

# Inicjalizacja Vadera
analyzer = SentimentIntensityAnalyzer()

def update_sentiment_data(sentiment, end_date):
    start_date=end_date+timedelta(minutes=-10)    
    start_epoch = int(start_date.timestamp())
    end_epoch = int(end_date.timestamp())
    no_submissions = False
    no_comments = False

    # KOMENTARZE

    # Zbieranie wpisów
    api_request_generator = api.search_comments(
        subreddit = 'Bitcoin', 
        filter=['body','score', 'id', 'link_id', 'parent_id', 'permalink', 'author', 'author_fullname', 'comms_num'],
        after = start_epoch, before=end_epoch)

    comments = pd.DataFrame([submission.d_ for submission in api_request_generator])
    if len(comments) > 0:
        comments['date'] = pd.to_datetime(comments['created_utc'], utc=True, unit='s')
    else:
        no_comments = True

    ### POSTY

    # Zbieranie wpisów    
    api_request_generator = api.search_submissions(
        subreddit = 'Bitcoin', 
        filter=['title','score', 'upvote_ratio', 'id', 'author', 'author_fullname', 'url', 'full_link', 'permalink', 'num_comments', 'num_crossposts', 'created_utc', 'selftext'],
        after = start_epoch, before=end_epoch)

    submissions = pd.DataFrame([submission.d_ for submission in api_request_generator])
    if len(submissions) > 0:
        submissions['date'] = pd.to_datetime(submissions['created_utc'], utc=True, unit='s')
    else:
        no_submissions = True

    # Tworzenie zestawu komentarzy reprezentującego cały okres
    all_comments = pd.DataFrame()
    if not no_comments:
        comments['year']=comments['date'].dt.year
        comments['month']=comments['date'].dt.month
        comments['day'] = comments['date'].dt.day
        comments['time'] = comments['date'].dt.date
        comments['time'].value_counts()
        all_comments = pd.concat([all_comments, comments])

        # Obliczanie compound sentiment w czasie
        all_comments['sentiment']=all_comments['body'].apply(lambda x: analyzer.polarity_scores(x))
        all_comments['sentiment_score']=all_comments['sentiment'].apply(lambda x: x['compound'])
        sentiments_over_time_comments = all_comments.groupby('time')['sentiment_score'].mean()[0]
    else:
        sentiments_over_time_comments = 0

    # Tworzenie zestawu tytułów wpisów reprezentującego cały okres
    all_submissions = pd.DataFrame()
    if not no_submissions:
        submissions['year']=submissions['date'].dt.year
        submissions['month']=submissions['date'].dt.month
        submissions['day'] = submissions['date'].dt.day
        submissions['time'] = submissions['date'].dt.date
        submissions['time'].value_counts()
        all_submissions = pd.concat([all_submissions, submissions])


        # Obliczanie compound sentiment w czasie - z tytułów postów 
        all_submissions['sentiment']=all_submissions['title'].apply(lambda x: analyzer.polarity_scores(x))
        all_submissions['sentiment_score']=all_submissions['sentiment'].apply(lambda x: x['compound'])
        sentiments_over_time_submissions = all_submissions.groupby('time')['sentiment_score'].mean()[0]
    else:
        sentiments_over_time_submissions = 0

    # Obliczanie średniego compound sentiment z postów i komentarzy
    # Średnia ważona o liczbę wpisów w kategorii
    if not no_submissions or not no_comments:
        sentiments_over_time_comments_and_submissions = (sentiments_over_time_submissions*len(all_submissions) + 
        sentiments_over_time_comments*len(all_comments))/(len(all_submissions) + len(all_comments))
    else:
        sentiments_over_time_comments_and_submissions = 0

    # dodawanie wyników do df z wynikami vadera 
    entry=pd.DataFrame({"date":[end_date],  "submissions":[sentiments_over_time_submissions], "comments":[sentiments_over_time_comments], 
    'submissions_and_comments':[sentiments_over_time_comments_and_submissions], 'period_start': start_date, 'period_end': end_date})
    sentiment=sentiment.append(entry, ignore_index = True)

    return sentiment

def get_balance():
    with open('balance.json', 'r') as f:
        balance = json.load(f)
    return balance

def dump_balance(balance):
    with open('balance.json', 'w') as outfile:
        json.dump(balance, outfile)

def get_transactions():
    try:
        with open('transactions.json', 'r') as f:
            transactions = json.load(f)
        return transactions
    except:
        return {}

def dump_transactions(transactions):
    with open('transactions.json', 'w') as outfile:
        json.dump(transactions, outfile)

def get_sentiment():
    with open('sentiment.pkl', 'rb') as f:
        return pickle.load(f)[0]

def dump_sentiment(sentiment):
    with open('sentiment.pkl', 'wb') as f:
        pickle.dump([sentiment], f)

def get_commission_amount():
    with open('commission_amount.pkl', 'rb') as f:
        return pickle.load(f)[0]

def dump_commission_amount(commission_amount):
    with open('commission_amount.pkl', 'wb') as f:
        pickle.dump([commission_amount], f)

def get_local_balance():
    with open('local_balance.json', 'r') as f:
        local_balance = json.load(f)
    return local_balance

def dump_local_balance(local_balance):
    with open('local_balance.json', 'w') as outfile:
        json.dump(local_balance, outfile)

def range_with_floats(start, stop, step):
    while stop > start:
        yield start
        start += step

def create_grid_lines(bottom_limit, upper_limit, line_dist):
    grid_lines = []
    for i in range_with_floats(bottom_limit, upper_limit, line_dist):
        grid_lines.append(i)
    return grid_lines

def get_current_position(current_price, grid_lines):
    for i in range(0, len(grid_lines) - 1):
        if(grid_lines[i] <= current_price and current_price < grid_lines[i+1]):
            lower_trigger = grid_lines[i]
            upper_trigger = grid_lines[i+1]
            return lower_trigger, upper_trigger
    return 0, 0

def find(lst, key, value): # do znajdowania waluty w słowniku słowników
    for i, dic in enumerate(lst):
        if dic[key] == value:
            return i
    return -1

def update_local_balance(local_balance, side, symbol, quantity, quote_quantity):
    if side == 'SELL':
        local_balance[find(local_balance, 'asset', symbol[0:3])]['free'] = float(local_balance[find(local_balance, 'asset', symbol[0:3])]['free']) - quantity
        local_balance[find(local_balance, 'asset', symbol[3:6])]['free'] = float(local_balance[find(local_balance, 'asset', symbol[3:6])]['free']) + float(quote_quantity)
        return local_balance
    elif side == 'BUY':
        local_balance[find(local_balance, 'asset', symbol[0:3])]['free'] = float(local_balance[find(local_balance, 'asset', symbol[0:3])]['free']) + quantity
        local_balance[find(local_balance, 'asset', symbol[3:6])]['free'] = float(local_balance[find(local_balance, 'asset', symbol[3:6])]['free']) - (quote_quantity)
        return local_balance

client = Client(api_key,
                private_key,
                 testnet=True) 

engine = create_engine('sqlite:///' + 'transactions.db') 
metadata = MetaData(engine)
metadata.reflect()

session_factory = sessionmaker(bind=engine)
session_scoped_factory = scoped_session(session_factory)
session = session_scoped_factory()

print(client.get_account()['balances'])

symbol = 'BTCUSDT'
upper_limit = 200000
bottom_limit = 0
line_dist = 250 # odległość między liniami - do wyregulowania
optimism_value = line_dist*0.2 # wartość optymizmu - do wyregulowania
order_value = 500 # w USDT, ile chcemy za każdym razem kupić/sprzedać - do wyregulowania
grid_lines = create_grid_lines(bottom_limit, upper_limit, line_dist)
sentiment = get_sentiment()
commission_amount = get_commission_amount()
wait_time = 60*10 # oczekiwanie między transakcjami - do wyregulowania
fee_rate = 0.001 # opłata transakcyjna 0.1%

# # Odpalić TYLKO za pierwszym uruchomieniem bota, jeśli lokalny portfel jest nieaktualny, 
# albo jeszcze nie jest zapisany 
# local_balance = client.get_account()['balances']
# dump_local_balance(local_balance) 

# # Odpalić TYLKO jeśli nie ma zapisanego lokalnie comission_amount (czyli startujemy od zera po prostu)
# dump_commission_amount(0.0)

# # Odpalić TYLKO jeśli nie ma zapisanego lokalnie sentymentu (ustawiamy sobie zero na początek)
# sentiment=pd.DataFrame({"date":[datetime.now() + timedelta(minutes=-10)],  "submissions":[0], 
# "comments":[0], 'submissions_and_comments':[0]})
# dump_sentiment(sentiment)

local_balance = get_local_balance() # W każdym innym przypadku po prostu pobieramy zapisany lokalny portfel

try:
    data = pd.DataFrame(client.get_ticker())
    lower_trigger, upper_trigger = get_current_position(float(data.loc[data['symbol'] == symbol]['openPrice'].values[0]), grid_lines)
    while True:
        # sprawdzenie sentymentu
        sentiment = update_sentiment_data(sentiment, datetime.now())
        
        if(sentiment['submissions_and_comments'][len(sentiment)-1]>=sentiment['submissions_and_comments'][len(sentiment)-2]):
            optimism = True
        else:
            optimism = False


        data = pd.DataFrame(client.get_ticker())
        current_price = float(data.loc[data['symbol'] == symbol]['openPrice'].values[0])
        if current_price > upper_trigger or (not optimism and current_price > upper_trigger - optimism_value):
            try:
                order = client.create_order(
                    symbol = symbol,
                    side = 'SELL',
                    type = 'MARKET',
                    quantity = round(order_value/current_price, 4)
                )
                transactions = get_transactions()
                balance = client.get_account()['balances']
                dump_balance(balance)
                local_balance = update_local_balance(local_balance, 'SELL', symbol, round(order_value/current_price, 4), float(order['cummulativeQuoteQty']))
                dump_local_balance(local_balance)
                transactions[time.time()] = {'Type': 'SELL', 'Symbol': symbol, 'Quantity': round(order_value/current_price, 4), 'Trigger Price': current_price, 
                'Transaction Price': order['fills'][0]['price'],'Quote quantity': order['cummulativeQuoteQty'], 'Binance Balance': balance, 'Local Balance': local_balance}
                dump_transactions(transactions)
                dump_sentiment(sentiment)
                commission_amount += fee_rate*order_value
                dump_commission_amount(commission_amount)
                print('Type: ' + 'SELL' + ' Symbol: ' + symbol + ' Quantity: ' + str(round(order_value/current_price, 4)) + ' Trigger Price: ' + str(current_price) + 
                ' Transaction Price: ' + order['fills'][0]['price'] +   ' Quote quantity: '+ order['cummulativeQuoteQty'] + ' Binance Balance: ' + str(json.dumps(balance)) +
                ' Local Balance: ' + str(json.dumps(local_balance)) + ' Time: ' + str(datetime.now()))
            except:
                print('Failed to create a SELL order. TIME:' + str(datetime.now()))
                traceback.print_exc()
                with open('traceback.txt', 'w+') as f:
                        f.write('Failed to create a SELL order. TIME:' + str(datetime.now()) + '\n')
                        traceback.print_exc(file=f)
            lower_trigger, upper_trigger = get_current_position(current_price, grid_lines)
        elif current_price < lower_trigger or (optimism and current_price < lower_trigger + optimism_value):
            try:
                order = client.create_order(
                    symbol = symbol,
                    side = 'BUY',
                    type = 'MARKET',
                    quantity = round(order_value/current_price, 4)
                )
                transactions = get_transactions()
                balance = client.get_account()['balances']
                dump_balance(balance)
                local_balance = update_local_balance(local_balance, 'BUY', symbol, round(order_value/current_price, 4), float(order['cummulativeQuoteQty']))
                dump_local_balance(local_balance)
                transactions[time.time()] = {'Type': 'BUY', 'Symbol': symbol, 'Quantity': round(order_value/current_price, 4), 'Price': current_price, 
                'Transaction Price': order['fills'][0]['price'],'Quote quantity': order['cummulativeQuoteQty'], 'Binance Balance': balance, 'Local Balance': local_balance}
                dump_transactions(transactions)
                dump_sentiment(sentiment)
                commission_amount += fee_rate*order_value
                dump_commission_amount(commission_amount)
                print('Type: ' + 'BUY' + ' Symbol: ' + symbol + ' Quantity: ' + str(round(order_value/current_price, 4)) + ' Trigger Price: ' + str(current_price) + 
                ' Transaction Price: ' + order['fills'][0]['price'] + ' Quote quantity: ' + order['cummulativeQuoteQty'] + ' Binance Balance: ' + str(json.dumps(balance)) +
                ' Local Balance: ' + str(json.dumps(local_balance)) + ' Time: ' + str(datetime.now()))
            except:
                print('Failed to create a BUY order. TIME:' + str(datetime.now()))
                traceback.print_exc()
                with open('traceback.txt', 'w+') as f:
                        f.write('Failed to create a BUY order. TIME:' + str(datetime.now()) + '\n')
                        traceback.print_exc(file=f)
            lower_trigger, upper_trigger = get_current_position(current_price, grid_lines)
        else:
            transactions = get_transactions()
            # balance = client.get_account()['balances']
            # dump_balance(balance)
            # local_balance = update_local_balance(local_balance, 'BUY', symbol, round(order_value/current_price, 4), order['cummulativeQuoteQty'])
            # dump_local_balance(local_balance)
            transactions[time.time()] = {'Type': 'NO TRANSACTION', 'Symbol': symbol, 'Price': current_price, 
            # 'Binance Balance': balance, 'Local Balance': local_balance
            }
            dump_transactions(transactions)
            dump_sentiment(sentiment)
            dump_commission_amount(commission_amount)
            print('No transaction. Current price = ' + str(current_price) + ' Time: ' + str(datetime.now()))
        sleep(wait_time)
except KeyboardInterrupt:
    dump_balance(client.get_account()['balances'])

    # SQL do ogarnięcia kiedyś tam
    # with engine.begin() as connection:
    #     pd.read_json('transactions.json').transpose().to_sql('transactions', con=connection, if_exists='append')

    # print('Save to database - start')
    # session.commit()
    # print('Save to database - end')

    print('Bot stopped working')
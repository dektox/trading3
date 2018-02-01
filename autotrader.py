from datetime import datetime
import time
import threading
import queue
import math

import ccxt
import telegram.ext as telegram

from utils import load_config, Console, Telegram


DEBUG = False
EXCHANGE_TIMEOUT = 1

config = load_config()
console = Console(log_file_name=config.log_file_path)
telebot = Telegram(telegram_bot_token=config.telegram_bot_token, telegram_chat_id=config.telegram_chat_id)
trader_commands = queue.Queue()


class BTCTradeUA:
    def __init__(self, exchange_config):
        self.exchange_config = exchange_config
        self.api = ccxt.btctradeua(config=self.exchange_config.to_dict())
        self.api.nonce = ccxt.Exchange.milliseconds
        self.api_lock = threading.Lock()

    def milliseconds(self):
        return self.api.milliseconds()

    def create_order(self, side, symbol, amount, price):
        with self.api_lock:
            while True:
                try:
                    return self.api.request(
                        path='{}/{}_{}'.format(side, symbol.split('/')[0], symbol.split('/')[1]).lower(),
                        api='private',
                        method='POST',
                        params={
                            'count': amount,
                            'price': price,
                            'currency1': symbol.split('/')[1],
                            'currency': symbol.split('/')[0],
                        }
                    )
                except ccxt.NetworkError:
                    time.sleep(0.25)

    def check_order(self, order_id):
        with self.api_lock:
            while True:
                try:
                    return self.api.request(
                        path='order/status/{}'.format(order_id),
                        api='private',
                        method='POST'
                    )
                except ccxt.NetworkError:
                    time.sleep(0.25)

    def delete_order(self, symbol, order_id):
        with self.api_lock:
            while True:
                try:
                    resp = self.check_order(order_id=order_id)
                    if resp['status'] == 'processing':
                        return self.api.request(
                            path='order/remove/{}_{}/{}'.format(symbol.split('/')[0], symbol.split('/')[1],
                                                                order_id).lower(),
                            api='private',
                            method='POST'
                        )
                    else:
                        return resp
                except ccxt.NetworkError:
                    time.sleep(0.25)

    def fetch_trade_book(self, symbol):
        with self.api_lock:
            while True:
                try:
                    return self.api.fetch_trades(symbol=symbol)
                except ccxt.NetworkError:
                    time.sleep(0.25)

    def fetch_order_book(self, symbol):
        with self.api_lock:
            while True:
                try:
                    return self.api.fetch_order_book(symbol=symbol)
                except ccxt.NetworkError:
                    time.sleep(0.25)

    def get_market_price(self, side, symbol, amount):
        with self.api_lock:
            while True:
                try:
                    if side == 'buy':
                        return self.api.request(
                            path='/ask/{}_{}'.format(symbol.split('/')[0], symbol.split('/')[1]).lower(),
                            api='private',
                            method='POST',
                            params={
                                'amount': amount
                            }
                        )
                    elif side == 'sell':
                        return self.api.request(
                            path='/bid/{}_{}'.format(symbol.split('/')[0], symbol.split('/')[1]).lower(),
                            api='private',
                            method='POST',
                            params={
                                'amount': amount
                            }
                        )
                except ccxt.NetworkError:
                    time.sleep(0.25)


class TradeBookWatcher(threading.Thread):
    def __init__(self, ex_api, target_user_trades, target_user_trades_fetched):
        super(TradeBookWatcher, self).__init__()
        self.daemon = True
        self.ex_api = ex_api
        self.target_user_trades = target_user_trades
        self.target_user_trades_fetched = target_user_trades_fetched

    def run(self):
        reported_orders_ids = []
        errors_count = 0
        while True:
            try:
                if not self.target_user_trades_fetched.is_set():
                    trades = self.ex_api.fetch_trade_book(symbol=config.target_pair)
                    for trade in sorted(trades, key=lambda t: t['timestamp']):
                        is_target_user = trade['info']['user'] in config.target_users_names
                        is_not_reported_yet = trade['id'] not in reported_orders_ids
                        is_mean = trade['amount'] >= config.min_mean_trade_amount
                        is_new = (self.ex_api.milliseconds() - trade['timestamp']) < config.max_trade_age*1000

                        if is_target_user and is_mean and is_not_reported_yet:
                            if is_new:
                                self.target_user_trades.put(trade)
                                reported_orders_ids.append(trade['id'])
                                console.log_order('New trade ({})'.format(trade['info']['user']), trade,
                                                  color=('yellow' if trade['side'] == 'sell' else 'blue'))
                                telebot.log_order('New trade ({})'.format(trade['info']['user']), trade)
                            else:
                                reported_orders_ids.append(trade['id'])
                                console.log_order('Old trade ({})'.format(trade['info']['user']), trade)
                                #telebot.log_order('Old trade ({})'.format(trade['info']['user']), trade)

                    self.target_user_trades_fetched.set()

                while True:
                    now = datetime.now()
                    if now.second % 10 == 0:
                        break
                    else:
                        time.sleep(0.5)
                    errors_count = 0
            except KeyboardInterrupt:
                break
            except SystemExit:
                raise
            except Exception as e:
                if DEBUG:
                    raise
                console.log('{:20}: {}'.format('OrderbookWatcher', str(e)), is_ok=0)
                telebot.log('{:20}: {}'.format('OrderbookWatcher', str(e)))
                errors_count += 1
                if errors_count > 10:
                    console.log('{:20}: {}'.format('OrderbookWatcher', 'Too many errors. Stopping'), is_ok=0)
                    telebot.log('{:20}: {}'.format('OrderbookWatcher', 'Too many errors. Stopping'))
                    break
                else:
                    time.sleep(1)


class Analyzer(threading.Thread):
    def __init__(self, ex_api, target_user_trades, target_user_trades_fetched):
        super(Analyzer, self).__init__()
        self.daemon = True
        self.ex_api = ex_api
        self.target_user_trades = target_user_trades
        self.target_user_trades_fetched = target_user_trades_fetched

    def run(self):
        errors_count = 0
        while True:
            try:
                self.target_user_trades_fetched.wait()

                target_user_trades_list = []
                while True:
                    try:
                        target_user_trades_list.append(self.target_user_trades.get_nowait())
                    except queue.Empty:
                        break
                if not target_user_trades_list:
                    continue

                total_amount_buy = math.fsum([trade['amount']
                                              for trade
                                              in target_user_trades_list
                                              if trade['side'] == 'buy'])

                total_amount_sell = math.fsum([trade['amount']
                                               for trade
                                               in target_user_trades_list
                                               if trade['side'] == 'sell'])

                weighted_price_buy = math.fsum([trade['amount']*trade['price']/total_amount_buy
                                                for trade
                                                in target_user_trades_list
                                                if trade['side'] == 'buy'])

                weighted_price_sell = math.fsum([trade['amount']*trade['price']/total_amount_sell
                                                 for trade
                                                 in target_user_trades_list
                                                 if trade['side'] == 'sell'])

                if (total_amount_buy and total_amount_sell) and math.fabs(1-(total_amount_buy/total_amount_sell)) < 0.2:
                    console.log('{:20}: {}'.format('Analyzer\'s idea', 'Fast buy/sell combination.'), n=1)
                    continue

                if total_amount_buy > total_amount_sell:
                    amount = (total_amount_buy - total_amount_sell) * config.order_amount_mult
                    price = self.ex_api.get_market_price(side='buy', symbol=config.target_pair, amount=amount)
                    command = {
                        'command_type': 'create_order',
                        'timestamp': self.ex_api.milliseconds(),
                        'symbol': config.target_pair,
                        'side': 'buy',
                        'amount': amount,
                        'price': float(price['end_price'])*config.buy_price_mult
                    }
                    trader_commands.put(command)
                    console.log_order('Analyzer\'s idea', command)
                    telebot.log_order('Analyzer\'s idea', command)
                else:
                    amount = (total_amount_sell - total_amount_buy) * config.order_amount_mult
                    price = self.ex_api.get_market_price(side='sell', symbol=config.target_pair, amount=amount)
                    command = {
                        'command_type': 'create_order',
                        'timestamp': self.ex_api.milliseconds(),
                        'symbol': config.target_pair,
                        'side': 'sell',
                        'amount': amount,
                        'price': float(price['end_price'])*config.sell_price_mult
                    }
                    trader_commands.put(command)
                    console.log_order('Analyzer\'s idea', command)

                errors_count = 0
            except KeyboardInterrupt:
                break
            except SystemExit:
                raise
            except Exception as e:
                if DEBUG:
                    raise
                console.log('{:20}: {}'.format('Analyzer', str(e)), is_ok=0)
                telebot.log('{:20}: {}'.format('Analyzer', str(e)))
                errors_count += 1
                if errors_count > 10:
                    console.log('{:20}: {}'.format('Analyzer', 'Too many errors. Stopping'), is_ok=0)
                    telebot.log('{:20}: {}'.format('Analyzer', 'Too many errors. Stopping'))
                    break
                else:
                    time.sleep(1)
            finally:
                self.target_user_trades_fetched.clear()


class Trader(threading.Thread):
    def __init__(self, ex_api):
        super(Trader, self).__init__()
        self.daemon = True
        self.config = config
        self.ex_api = ex_api
        self.orders = {}

    def run(self):
        errors_count = 0
        while True:
            try:
                try:
                    command = trader_commands.get(timeout=5)
                except queue.Empty:
                    command = None

                if command and command['command_type'] == 'create_order':
                    resp = self.ex_api.create_order(side=command['side'], symbol=command['symbol'],
                                                    amount=command['amount'], price=command['price'])
                    if resp['status'] is True:
                        self.orders[resp['order_id']] = command
                        console.log_order('NEW order', command)
                        telebot.log_order('NEW order', command)
                    else:
                        console.log('{:20}: {}'.format('Trader', 'Failed to create order.'), is_ok=0)

                for order_id in list(self.orders.keys()):
                    order = self.orders[order_id]
                    resp = self.ex_api.check_order(order_id=order_id)
                    if resp['status'] == 'processed':
                        del self.orders[order_id]
                        console.log_order('FIN order', order, color='green')
                        telebot.log_order('FIN order', order)
                    elif (self.ex_api.milliseconds()-order['timestamp']) > self.config.max_order_age*1000:
                        console.log_order('DEL order', order, color='red')
                        telebot.log_order('DEL order', order)
                        self.ex_api.delete_order(symbol=order['symbol'], order_id=order_id)

                errors_count = 0
            except KeyboardInterrupt:
                break
            except SystemExit:
                raise
            except Exception as e:
                if DEBUG:
                    raise
                console.log('{:20}: {}'.format('Trader', str(e)), is_ok=0)
                telebot.log('{:20}: {}'.format('Trader', str(e)))
                errors_count += 1
                if errors_count > 10:
                    console.log('{:20}: {}'.format('Trader', 'Too many errors. Stopping'), is_ok=0)
                    telebot.log('{:20}: {}'.format('Trader', 'Too many errors. Stopping'))
                    break
                else:
                    time.sleep(1)


class MarketPriceWatcher(threading.Thread):
    def __init__(self, ex_api):
        super(MarketPriceWatcher, self).__init__()
        self.daemon = True
        self.ex_api = ex_api

    def run(self):
        errors_count = 0
        while True:
            try:
                price_buy = self.ex_api.get_market_price(side='buy', symbol=config.target_pair,
                                                         amount=1000.0)['got_sum']
                price_sell = self.ex_api.get_market_price(side='sell', symbol=config.target_pair,
                                                          amount=1000.0)['cost_sum']

                console.log('{:20}: {}'.format(
                    'Market prices', '[BUY: {:.8f}    SELL: {:.8f}]'.format(float(price_buy)/1000.0,
                                                                            float(price_sell)/1000.0)))

                while True:
                    now = datetime.now()
                    if now.second != 0 and now.second % 3 == 0:
                        break
                    else:
                        time.sleep(0.5)
                errors_count = 0
            except KeyboardInterrupt:
                break
            except SystemExit:
                raise
            except Exception as e:
                if DEBUG:
                    raise
                console.log('{:20}: {}'.format('PriceWatcher', str(e)), is_ok=0)
                telebot.log('{:20}: {}'.format('PriceWatcher', str(e)))
                errors_count += 1
                if errors_count > 10:
                    console.log('{:20}: {}'.format('PriceWatcher', 'Too many errors. Stopping'), is_ok=0)
                    telebot.log('{:20}: {}'.format('PriceWatcher', 'Too many errors. Stopping'))
                    break
                else:
                    time.sleep(1)


exchange_api = BTCTradeUA(exchange_config=config.btctradeua)


def telegram_get_market_prices(bot, update):
    cmd_line = update.message.text.split(' ')
    try:
        amount, symbol = float(cmd_line[1]), str(cmd_line[2]).upper()
    except (ValueError, IndexError):
        bot.send_message(chat_id=update.message.chat_id, parse_mode='HTML',
                         text='<b>Wrong command format. Try this:</b><pre>/prices amount pair</pre>')
        return

    price_buy = float(exchange_api.get_market_price(side='buy', symbol=symbol,
                                                    amount=amount)['got_sum'])/amount
    price_sell = float(exchange_api.get_market_price(side='sell', symbol=symbol,
                                                     amount=amount)['cost_sum'])/amount

    bot.send_message(chat_id=update.message.chat_id, parse_mode='HTML',
                     text='<b>Market prices ({:.4f} {}):</b><pre>BUY: {:.8f}\nSELL: {:.8f}</pre>'
                     .format(amount, symbol, price_buy, price_sell))


def telegram_create_buy_order(bot, update):
    cmd_line = update.message.text.split(' ')
    try:
        if len(cmd_line) == 3:
            amount, symbol, price = float(cmd_line[1]), str(cmd_line[2]).upper(), None
        elif len(cmd_line) == 4:
            amount, symbol, price = float(cmd_line[1]), str(cmd_line[2]).upper(), float(cmd_line[3])
        else:
            raise IndexError
    except (ValueError, IndexError):
        bot.send_message(chat_id=update.message.chat_id, parse_mode='HTML',
                         text='<b>Wrong command format. Try this:</b><pre>/buy amount pair [price]</pre>')
        return

    if not price:
        price = float(exchange_api.get_market_price(side='buy', symbol=config.target_pair, amount=amount)['end_price'])

    order = {
        'command_type': 'create_order',
        'timestamp': exchange_api.milliseconds(),
        'symbol': config.target_pair,
        'side': 'buy',
        'amount': amount,
        'price': price
    }
    trader_commands.put(order)


def telegram_create_sell_order(bot, update):
    cmd_line = update.message.text.split(' ')
    try:
        if len(cmd_line) == 3:
            amount, symbol, price = float(cmd_line[1]), str(cmd_line[2]).upper(), None
        elif len(cmd_line) == 4:
            amount, symbol, price = float(cmd_line[1]), str(cmd_line[2]).upper(), float(cmd_line[3])
        else:
            raise IndexError
    except (ValueError, IndexError):
        bot.send_message(chat_id=update.message.chat_id, parse_mode='HTML',
                         text='<b>Wrong command format. Try this:</b><pre>/sell amount pair [price]</pre>')
        return

    if not price:
        price = float(exchange_api.get_market_price(side='sell', symbol=config.target_pair, amount=amount)['end_price'])

    order = {
        'command_type': 'create_order',
        'timestamp': exchange_api.milliseconds(),
        'symbol': config.target_pair,
        'side': 'sell',
        'amount': amount,
        'price': price
    }
    trader_commands.put(order)


def main():
    try:
        console.log('Starting...')
        target_user_trades = queue.Queue()
        target_user_trades_fetched = threading.Event()

        trader = Trader(ex_api=exchange_api)
        trader.start()

        analyzer = Analyzer(ex_api=exchange_api,
                            target_user_trades=target_user_trades,
                            target_user_trades_fetched=target_user_trades_fetched)
        analyzer.start()

        trade_book_loader = TradeBookWatcher(ex_api=exchange_api,
                                             target_user_trades=target_user_trades,
                                             target_user_trades_fetched=target_user_trades_fetched)
        trade_book_loader.start()

        market_price_watcher = MarketPriceWatcher(ex_api=exchange_api)
        market_price_watcher.start()

        telebot.add_handler(telegram.CommandHandler(command='prices', callback=telegram_get_market_prices))
        telebot.add_handler(telegram.CommandHandler(command='buy', callback=telegram_create_buy_order))
        telebot.add_handler(telegram.CommandHandler(command='sell', callback=telegram_create_sell_order))
        telebot.start()

        console.log('Ready!')
        try:
            while trader.is_alive() \
                    and analyzer.is_alive() \
                    and trade_book_loader.is_alive() \
                    and market_price_watcher.is_alive() \
                    and telebot.is_alive():
                time.sleep(5)
        except:
            telebot.stop()
            while telebot.is_alive():
                time.sleep(1)
            raise
    except KeyboardInterrupt:
        return
    except:
        raise
    finally:
        console.log('', n=1)


if __name__ == '__main__':
    main()

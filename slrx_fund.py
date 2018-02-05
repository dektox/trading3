import threading
from traceback import format_exc

import ccxt
import telegram.ext as telegram

from utils import load_config, Console, Telegram

DEFAULT_CONFIG_PATH = 'trading_configs/slrx_fund.json'

config = load_config(default_config_path=DEFAULT_CONFIG_PATH)
console = Console(log_file_name=config.log_file_path)
telebot = Telegram(config=config.telegram)

exchanges = {}


class Exchange:
    def __init__(self, api):
        self._api = api
        self._api.nonce = self._api.milliseconds
        self.lock = threading.Lock()

    def __getattr__(self, item):
        with self.lock:
            return getattr(self._api, item)


class TelegramCommands:
    @staticmethod
    def get_chat_name(chat):
        return chat.title or chat.username or chat.first_name + ' ' + chat.last_name

    @staticmethod
    def start(bot, update):
        console.log('{:<15} {:<20}: {}'.format(update.message.chat_id,
                                               TelegramCommands.get_chat_name(update.message.chat),
                                               update.message.text), n=1)
        TelegramCommands.show_help(bot, update)

    @staticmethod
    def show_help(bot, update):
        msg = '<pre>'
        msg += 'View balance\n'
        msg += '  /balance [grouping] [blocking] [symbol]\n'
        msg += '    grouping: exchange name, sum, (every)\n'
        msg += '    blocking: free, used, (total)\n'
        msg += '    symbol:   currency symbol, (any)\n'
        msg += '\n'
        msg += '  example: /balance bitstamp used\n'
        msg += '  example: /balance bittrex BTC\n'
        msg += '  example: /balance sum\n'
        msg += '</pre>'

        bot.send_message(chat_id=update.message.chat_id, parse_mode='HTML', text=msg)

    @staticmethod
    def get_balance(bot, update):
        try:
            exchange = 'every'
            blocking = 'total'
            symbol = None
            for arg in update.message.text.split(' ')[1:]:
                if arg in ('free', 'used', 'total'):
                    blocking = arg
                elif (arg in ('every', 'sum')) or (arg in exchanges.keys()):
                    exchange = arg
                elif arg.isupper():
                    symbol = arg

            console.log('{:<15} {:<20}: /balance [exchange: {}, symbol: {}, blocking: {}]'
                        .format(update.message.chat_id, TelegramCommands.get_chat_name(update.message.chat),
                                str(exchange), str(symbol), str(blocking)), n=1)
            if exchange == 'every':
                for exchange in exchanges.keys():
                    balance = exchanges[exchange].fetch_balance()
                    msg = '<pre>'
                    msg += 'Balance {} ({})'.format(blocking, exchange)
                    if symbol:
                        if symbol in balance[blocking]:
                            msg += '\n{}: {:.8f}'.format(symbol, balance[blocking][symbol])
                    else:
                        for sym, bal in balance[blocking].items():
                            if bal:
                                msg += '\n{}: {:.8f}'.format(sym, bal)
                    msg += '</pre>'
                    bot.send_message(chat_id=update.message.chat_id, parse_mode='HTML', text=msg)

            elif exchange == 'sum':
                summary = {}
                for exchange in exchanges.keys():
                    balance = exchanges[exchange].fetch_balance()
                    if symbol:
                        if symbol in balance[blocking]:
                            if symbol in summary.keys():
                                summary[symbol] += balance[blocking][symbol]
                            else:
                                summary[symbol] = balance[blocking][symbol]
                    else:
                        for sym, bal in balance[blocking].items():
                            if sym in summary.keys():
                                summary[sym] += bal
                            else:
                                summary[sym] = bal
                msg = '<pre>'
                msg += 'Balance {} ({})'.format(blocking, 'summary')
                if symbol:
                    msg += '\n{}: {:.8f}'.format(symbol, summary[symbol])
                else:
                    for sym, bal in summary.items():
                        if bal:
                            msg += '\n{}: {:.8f}'.format(sym, bal)
                msg += '</pre>'
                bot.send_message(chat_id=update.message.chat_id, parse_mode='HTML', text=msg)

            else:
                balance = exchanges[exchange].fetch_balance()
                msg = '<pre>'
                msg += 'Balance {} ({})'.format(blocking, exchange)
                if symbol:
                        if symbol in balance[blocking]:
                            msg += '\n{}: {:.8f}'.format(symbol, balance[blocking][symbol])
                else:
                    for sym, bal in balance[blocking].items():
                        if bal:
                            msg += '\n{}: {:.8f}'.format(sym, bal)
                msg += '</pre>'

                bot.send_message(chat_id=update.message.chat_id, parse_mode='HTML', text=msg)

        except (IndexError, ValueError):
            if config.debug:
                bot.send_message(chat_id=update.message.chat_id, parse_mode='HTML',
                                 text='<pre>{}</pre>'.format(format_exc()))
            bot.send_message(chat_id=update.message.chat_id, parse_mode='HTML',
                             text='Invalid command. Type /help to get help.')


def main():
    console.log('Starting...')

    exchanges['bitstamp'] = Exchange(ccxt.bitstamp(config=config.bitstamp.to_dict()))
    exchanges['binance'] = Exchange(ccxt.binance(config=config.binance.to_dict()))
    exchanges['bittrex'] = Exchange(ccxt.bittrex(config=config.bittrex.to_dict()))
    exchanges['cryptopia'] = Exchange(ccxt.cryptopia(config=config.cryptopia.to_dict()))
    exchanges['hitbtc'] = Exchange(ccxt.hitbtc2(config=config.hitbtc.to_dict()))

    telebot.add_handler(telegram.CommandHandler(command='balance', callback=TelegramCommands.get_balance))
    telebot.add_handler(telegram.CommandHandler(command='start', callback=TelegramCommands.start))
    telebot.add_handler(telegram.CommandHandler(command='help', callback=TelegramCommands.show_help))
    telebot.start()

    console.log('Ready...')

    telebot.idle()


if __name__ == '__main__':
    main()

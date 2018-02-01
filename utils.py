import os
import json
from sys import argv
from time import time, strftime, localtime

import telegram
import telegram.ext
from colorama import Fore as colors


class Config:
    @staticmethod
    def _to_config(from_item):
        if isinstance(from_item, dict):
            res = Config()
            for key, val in from_item.items():
                res.set(key, Config._to_config(val))
        elif isinstance(from_item, list):
            res = [Config._to_config(val) for val in from_item]
        elif isinstance(from_item, tuple):
            res = tuple([Config._to_config(val) for val in from_item])
        else:
            res = from_item
        return res

    @staticmethod
    def _convert_to_std(from_item):
        if isinstance(from_item, Config):
            return Config._convert_to_std(from_item.__dict__)
        elif isinstance(from_item, dict):
            return {
                key: Config._convert_to_std(val)
                for key, val
                in from_item.items()
            }
        elif isinstance(from_item, list):
            return [
                Config._convert_to_std(el)
                for el
                in from_item
            ]
        elif isinstance(from_item, tuple):
            return tuple([
                Config._convert_to_std(el)
                for el
                in from_item
            ])
        else:
            return from_item

    def __init__(self, from_item=None):
        self.update(from_item)

    def __getattr__(self, item):
        self.__dict__.get(item, None)

    def set(self, key, val):
        self.__dict__[key] = val

    def update(self, from_item):
        if from_item:
            if isinstance(from_item, dict):
                for key, val in from_item.items():
                    self.set(key, Config._to_config(val))
            elif isinstance(from_item, Config):
                for key, val in from_item.to_dict().items():
                    self.set(key, Config._to_config(val))

    def to_dict(self):
        return Config._convert_to_std(self)

    def __str__(self):
        stringed = ', '.join(['{}: {}'.format(key, str(val)) for key, val in self.to_dict().items()])
        return 'Config({})'.format(stringed)


class Telegram:
    def __init__(self, telegram_bot_token, telegram_chat_id):
        self.chat_id = telegram_chat_id
        self.bot = telegram.Bot(token=telegram_bot_token)
        self._updater = telegram.ext.Updater(bot=self.bot)

    def log(self, msg):
        self.bot.send_message(chat_id=self.chat_id, text=msg, parse_mode='HTML')

    def log_order(self, comment, order):
        msg = '<b>{:20}:</b>\n<pre>{}</pre>'.format(
            str(comment)[:20],
            'DATE: {}\nPAIR: {}\nSIDE: {}\nAMNT: {:.8f}\nPRICE: {:.8f}'.format(
                str(strftime('%Y-%m-%d %H:%M:%S', localtime(order['timestamp'] / 1000)))[:20],
                str(order['symbol'])[:10],
                str(order['side'])[:5],
                order['amount'], order['price'])
        )
        self.log(msg=msg)

    def add_handler(self, handler):
        self._updater.dispatcher.add_handler(handler)

    def start(self):
        self._updater.start_polling(clean=True, timeout=30.0, bootstrap_retries=-1)

    def stop(self):
        if self.is_alive():
            self._updater.stop()

    def is_alive(self):
        return self._updater.running


class Console:
    def __init__(self, log_file_name=None,
                 console_width=150, silent=False):
        self.log_file_name = log_file_name
        self.console_width = console_width
        self.silent = silent

    def log(self, msg, n=False, is_ok=True, div=False, color=None):
        if color == 'red':
            color = colors.LIGHTRED_EX
        elif color == 'blue':
            color = colors.CYAN
        elif color == 'green':
            color = colors.LIGHTGREEN_EX
        elif color == 'yellow':
            color = colors.LIGHTYELLOW_EX

        if div:
            print((color or colors.LIGHTYELLOW_EX) + ('='*100) + colors.RESET, end='\n', flush=True)
        if msg:
            text = '{} {:<300}'.format(strftime('%Y-%m-%d %H:%M:%S'), str(msg))
        else:
            text = ' ' * self.console_width

        if not is_ok:
            start = '\r'+(color or colors.LIGHTRED_EX)
            end = (colors.RESET + '\n')
        else:
            start = ('\r'+(color or colors.RESET)) if n else ('\r' + (color or colors.RESET))
            end = (colors.RESET+'\n') if n or div else colors.RESET

        if not self.silent:
            print(start + text[:self.console_width] + end, end='', flush=True)

        if self.log_file_name and text and (n or not is_ok):
            with open(self.log_file_name, 'a+', encoding='utf8') as log_file:
                log_file.write(text + '\n')
                log_file.flush()

    def log_order(self, comment, order, color=None):
        msg = '{:20}: {}'.format(
            str(comment)[:20],
            '[DATE: {:20} PAIR: {:10} SIDE: {:5} AMNT: {:15.8f} PRICE: {:15.8f}]'.format(
                str(strftime('%Y-%m-%d %H:%M:%S', localtime(order['timestamp'] / 1000)))[:20],
                str(order['symbol'])[:10],
                str(order['side'])[:5],
                order['amount'], order['price'])
        )
        self.log(msg=msg, color=color, n=True)

    def wait_for_kbhit(self, comment='Press any key to continue...', is_ok=True):
        text = '{} {:<300}'.format(strftime('%Y-%m-%d %H:%M:%S'), str(comment))[:self.console_width]

        start = '\r' + (colors.LIGHTYELLOW_EX if is_ok else colors.LIGHTRED_EX)
        end = colors.RESET

        try:
            input(start + text + end)
        except SyntaxError:
            pass
        except KeyboardInterrupt:
            exit(0)

    @staticmethod
    def clear():
        os.system('clear')


class TimeCounter:
    def __init__(self):
        self._started_time = None

    def start(self):
        self._started_time = time()

    def elapsed(self):
        return time()-self._started_time


def parse_value(x):
    if x == 'True':
        return True
    elif x == 'False':
        return False
    elif '.' in x:
        try:
            return float(x)
        except:
            return x
    else:
        try:
            return int(x)
        except:
            return x


def parse_value_from_str(x):
    if '.' in x:
        try:
            return float(x)
        except:
            return x
    else:
        try:
            return int(x)
        except:
            return x


def parse_options_str(argstr):
    arg_list = (' '+argstr).split(' -')
    argd = {}
    for arg in arg_list:
        if arg:
            value = parse_value_from_str(' '.join(arg.split(' ')[1:]))
            name = arg.split(' ')[0]
            if not value:
                value = True
            argd[name] = value
    return argd


def load_config(default_config_path=None, parse_cmd_line=True):
    if parse_cmd_line:
        cmd_line_config = Config(parse_options_str(' '.join(argv[1:])))
    else:
        cmd_line_config = Config()

    if cmd_line_config.config:
        with open(cmd_line_config.config) as main_config_file:
            main_config = Config(json.load(main_config_file))
    else:
        main_config = Config()

    if default_config_path:
        with open(default_config_path) as default_config_file:
            default_config = Config(json.load(default_config_file))
    else:
        default_config = Config()

    if default_config.include:
        for included_config_path in default_config.include[:]:
            included_config = load_config(default_config_path=included_config_path, parse_cmd_line=False)
            default_config.update(included_config)

    if main_config.include:
        for included_config_path in main_config.include[:]:
            included_config = load_config(default_config_path=included_config_path, parse_cmd_line=False)
            main_config.update(included_config)

    if cmd_line_config.include:
        for included_config_path in cmd_line_config.include[:]:
            included_config = load_config(default_config_path=included_config_path, parse_cmd_line=False)
            cmd_line_config.update(included_config)

    config = Config()
    config.update(default_config)
    config.update(main_config)
    config.update(cmd_line_config)

    return config


from datetime import datetime
from time import sleep

import ccxt

from utils import load_config, Console
from database import Postgres


DEBUG = False
DEFAULT_CONFIG = 'trades_history_saver.json'
console = Console(log_file_name='full_log.log')
cout = console.log


def save_btctradeua_trades(config, db, api, pairs):
    for symbol in pairs:
        cout('Loading btctradeua trades for pair {}...'.format(symbol))
        trades = api.fetch_trades(symbol=symbol)
        trades_count = len(trades)
        for trade_index, trade in enumerate(trades):
            cout('Saving btctradeua trades for pair {} ({}/{})...'.format(symbol,
                                                                          trade_index + 1, trades_count))
            parsed_trade = {
                'order_id': trade['id'],
                'order_date': trade['datetime'],
                'symbol': symbol,
                'user_name': trade['info']['user'],
                'order_side': trade['side'],
                'amount_base': trade['info']['amnt_base'],
                'amount_trade': trade['info']['amnt_trade'],
                'price': trade['price'],
            }
            sql = "  INSERT INTO {} ({})".format(config.table_btctradeua_history,
                                                 ','.join(parsed_trade.keys()))
            sql += " VALUES (%({})s)".format(')s,%('.join(parsed_trade.keys()))
            sql += " ON CONFLICT DO NOTHING"

            db.execute_nofetch(sql=sql, data=parsed_trade)
        sleep(1)
        cout('Saved btctradeua trades for pair {} ({})'.format(symbol, trades_count), n=1)
    db.commit()


def main():
    errors_count = 0
    while True:
        try:
            cout('Loading config "{}"...'.format(DEFAULT_CONFIG))
            config = load_config(DEFAULT_CONFIG)
            cout('Loaded config "{}"'.format(DEFAULT_CONFIG), n=1)

            cout('Connecting to "{}@{}:{}"...'.format(config.db.dbname, config.db.host, config.db.port))
            db = Postgres(config=config.db)
            db.initialize()
            cout('Connected to "{}@{}:{}"'.format(config.db.dbname, config.db.host, config.db.port), n=1)

            cout('Connecting to btctradeua API...')
            btctradeua_api = ccxt.btctradeua(config=config.btctradeua.to_dict())
            cout('Connected to btctradeua API', n=1)

            cout('Loading btctradeua pairs list...')
            btctradeua_pairs = list(btctradeua_api.load_markets(reload=True).keys())
            cout('There are {} pairs on btctradeua'.format(len(btctradeua_pairs)), n=1)

            everything_is_actual = True
            while everything_is_actual:
                cout('', n=1)
                save_btctradeua_trades(config=config, db=db, api=btctradeua_api, pairs=btctradeua_pairs)

                cout('Waiting...')
                while True:
                    now = datetime.now()
                    if now.minute == 0 and now.second == 0:
                        everything_is_actual = False
                        break
                    elif now.minute % 10 == 0 and now.second == 0:
                        break
                    else:
                        sleep(0.5)
                errors_count = 0
        except KeyboardInterrupt:
            break
        except Exception as ex:
            if DEBUG:
                raise
            cout(str(ex), is_ok=0)
            #errors_count += 1
            #if errors_count > 10:
            #    cout('Too many errors. Stopping', is_ok=0)
            #    break
        finally:
            # noinspection PyBroadException
            try:
                db.destroy()
            except:
                pass


if __name__ == '__main__':
    main()

from utils import load_config, Console
from database import Postgres

DEFAULT_CONFIG = 'trading_configs/trades_history_manager.json'

config = load_config(DEFAULT_CONFIG)
console = Console(log_file_name=config.log_file_path)
database = Postgres(config.db)
cout = Console().log


def set_tables(sql):
    sql = sql.replace('#prices#', config.schema.prices)
    sql = sql.replace('#orders#', config.schema.orders)
    sql = sql.replace('#orders_norm#', config.schema.orders_norm)
    sql = sql.replace('#balances#', config.schema.balances)
    sql = sql.replace('#users#', config.schema.users)
    sql = sql.replace('#min_orders_count#', str(config.min_orders_count))
    sql = sql.replace('#min_order_date#', str(config.min_order_date))
    sql = sql.replace('#max_order_date#', str(config.max_order_date))
    sql = sql.replace('#user_name#', str(config.user_name))
    sql = sql.replace('#symbol#', str(config.symbol))

    return sql


def extract_users_stats_table():
    cout('Extracting users stats...')
    with open('sql/extract_users_stats.sql', encoding='utf8') as sql_file:
        sql = sql_file.read()
        sql = set_tables(sql)
        users_stats = database.execute(sql)
    fields_list = ['user_name', 'symbol', 'buys_count', 'sells_count',
                   'avg_buy_amount', 'avg_sell_amount', 'total_profit',
                   'sh', 'active_price_change_corr_abs', 'active_price_change_corr_bin']
    cout('Saving users stats...')
    with open('users_stats/users_stats.csv', 'w+', encoding='utf8') as users_stats_file:
        users_stats_file.write(';'.join(fields_list)+'\n')
        for user in users_stats:
            users_stats_file.write(';'.join([str(val if val is not None else '') for val in user]) + '\n')
        cout('Extracted users stats', n=1)


def extract_user_activity_table(user_name, symbol):
    cout('Extracting {}, {} activity...'.format(user_name, symbol))
    with open('sql/extract_user_activity.sql', encoding='utf8') as sql_file:
        sql = sql_file.read()
        sql = set_tables(sql)
        users_stats = database.execute(sql)
    fields_list = ['order_date', 'price', 'price_change_1m',
                   'balance_trade', 'balance_base', 'balance_total', 'balance_change_total',
                   'was_active']
    cout('Saving {}, {} activity...'.format(user_name, symbol))
    with open('users_stats/{}_{}_{}.csv'.format(user_name, symbol.split('/')[0], symbol.split('/')[1]),
              'w+', encoding='utf8') as users_stats_file:
        users_stats_file.write(';'.join(fields_list)+'\n')
        for row in users_stats:
            users_stats_file.write(';'.join([str(val if val is not None else '') for val in row]) + '\n')
        cout('Extracted {}, {} activity'.format(user_name, symbol), n=1)


def update_orders_table():
    cout('Updating orders...')
    with open('sql/update_orders.sql', encoding='utf8') as update_sql:
        sql = update_sql.read()
        sql = set_tables(sql)
        database.execute(sql)
        database.commit()
    cout('Orders updated', n=1)


def update_orders_norm_table():
    cout('Updating normalized orders...')
    with open('sql/update_orders_norm.sql', encoding='utf8') as update_sql:
        sql = update_sql.read()
        sql = set_tables(sql)
        database.execute(sql)
        database.commit()
    cout('Normalized orders updated', n=1)


def update_prices_table():
    cout('Updating prices...')
    with open('sql/update_prices.sql', encoding='utf8') as update_sql:
        sql = update_sql.read()
        sql = set_tables(sql)
        database.execute(sql)
        database.commit()
    cout('Prices updated', n=1)


def main():
    if config.help:
        cout('=== Trader activity aggregator ===', n=1, color='green')
        cout('Available commands:', n=1)
        cout('    Add new trades to the tables.', n=1)
        cout('        python trades_history_manager.py -update_all', n=1)
        cout('    Get aggregated stats on activity of the traders.', n=1)
        cout('        python trades_history_manager.py -stats', n=1)
        cout('    Get aggregated stats on activity of the choosen trader.', n=1)
        cout('        python trades_history_manager.py -activity -user_name TRADER_NAME -symbol TRADE/BASE', n=1)
        cout('', n=1)

    if config.update_all:
        update_orders_table()
        update_prices_table()
        update_orders_norm_table()

    if config.update_orders:
        update_orders_table()

    if config.update_orders_norm:
        update_orders_norm_table()

    if config.update_prices:
        update_prices_table()

    if config.stats:
        extract_users_stats_table()

    if config.activity:
        extract_user_activity_table(user_name=config.user_name, symbol=config.symbol)


if __name__ == '__main__':
    main()

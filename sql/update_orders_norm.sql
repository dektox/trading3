INSERT INTO #orders_norm#
SELECT
        date_trunc('minute', order_date) AS order_date,
        user_name,
        symbol,
        CASE balance_change_trade < 0
            WHEN TRUE THEN 'sell'
            WHEN FALSE THEN 'buy'
            ELSE NULL
        END AS order_side,
        balance_change_trade,
        balance_change_base,
        price*balance_trade
            - lag(price) OVER (PARTITION BY user_name, symbol ORDER BY order_date)
            * lag(balance_trade) OVER (PARTITION BY user_name, symbol ORDER BY order_date)
            + balance_change_base AS balance_change_total,
        price,
        balance_base,
        balance_trade,
        price*balance_trade+balance_base AS balance_total
    FROM
    (
        SELECT
            date_trunc('minute', order_date) AS order_date,
            user_name,
            symbol,
            sum(balance_change_base) OVER one_user_and_pair_sorted AS balance_base,
            sum(balance_change_trade) OVER one_user_and_pair_sorted AS balance_trade,
            sum(balance_change_trade) OVER one_user_and_pair_and_minute AS balance_change_trade,
            sum(balance_change_base) OVER one_user_and_pair_and_minute AS balance_change_base,
            row_number() OVER one_user_and_pair_and_minute_sorted AS order_number_in_minute,
            (
                SELECT price
                FROM #prices#
                WHERE TRUE
                    AND #orders#.symbol=#prices#.symbol
                    AND #orders#.order_date>=#prices#.order_date
                ORDER BY order_date DESC
                LIMIT 1
            ) AS price
        FROM #orders#
        WINDOW
            one_user_and_pair_and_minute_sorted AS (
                PARTITION BY user_name, symbol, date_trunc('minute', order_date)
                ORDER BY order_date DESC
            ),
            one_user_and_pair_and_minute AS (
                PARTITION BY user_name, symbol, date_trunc('minute', order_date)
            ),
            one_user_and_pair_sorted AS (
                PARTITION BY user_name, symbol
                ORDER BY order_date
            )
    ) AS _balances
    WHERE order_number_in_minute = 1
ON CONFLICT DO NOTHING
;

SELECT
    order_date::timestamp,

    (
        SELECT price
        FROM btctradeua.history_prices
        WHERE TRUE
            AND btctradeua.history_prices.symbol='#symbol#'
            AND btctradeua.history_prices.order_date<=period.order_date
            ORDER BY order_date DESC
            LIMIT 1
    ) AS price,

    COALESCE(
    (
        SELECT price
        FROM btctradeua.history_prices
        WHERE TRUE
            AND btctradeua.history_prices.symbol='#symbol#'
            AND btctradeua.history_prices.order_date<=period.order_date
            ORDER BY order_date DESC
            LIMIT 1
    ) - (
        SELECT price
        FROM btctradeua.history_prices
        WHERE TRUE
            AND btctradeua.history_prices.symbol='#symbol#'
            AND btctradeua.history_prices.order_date<period.order_date
        ORDER BY order_date DESC
        LIMIT 1
    ), 0) AS price_change_1m,

    (
        SELECT balance_trade
        FROM #trades_norm#
        WHERE
            #trades_norm#.order_date<=period.order_date
            AND user_name='#user_name#'
            AND symbol='#symbol#'
        ORDER BY order_date DESC
        LIMIT 1
    ) AS balance_trade,

    (
        SELECT balance_base
        FROM #trades_norm#
        WHERE
            #trades_norm#.order_date<=period.order_date
            AND user_name='#user_name#'
            AND symbol='#symbol#'
        ORDER BY order_date DESC
        LIMIT 1
    ) AS balance_base,

    (
        SELECT balance_total
        FROM #trades_norm#
        WHERE
            #trades_norm#.order_date<=period.order_date
            AND user_name='#user_name#'
            AND symbol='#symbol#'
        ORDER BY order_date DESC
        LIMIT 1
    ) AS balance_total,

    COALESCE((
        SELECT balance_change_total
        FROM #trades_norm#
        WHERE
            #trades_norm#.order_date=period.order_date
            AND user_name='#user_name#'
            AND symbol='#symbol#'
        ORDER BY order_date DESC
        LIMIT 1
    ), 0) AS balance_change_total,

    COALESCE((
        SELECT
            CASE
            WHEN balance_change_trade < 0 THEN -1
            WHEN balance_change_trade > 0 THEN 1
            ELSE NULL
            END
        FROM #trades_norm#
        WHERE
            #trades_norm#.order_date=period.order_date
            AND user_name='#user_name#'
            AND symbol='#symbol#'
        LIMIT 1
    ), 0) AS was_active
FROM
    generate_series('#min_order_date#'::timestamp with time zone, '#max_order_date#'::timestamp with time zone, '1 minute') AS period(order_date)

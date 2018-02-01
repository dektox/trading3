WITH
    _selected_users AS (
        SELECT user_name, symbol
        FROM (
            SELECT user_name, symbol, count(*) AS cnt
            FROM #orders_norm#
            WHERE TRUE
                AND order_date >= '#min_order_date#'::timestamp with time zone
                AND order_date <= '#max_order_date#'::timestamp with time zone
            GROUP BY user_name, symbol
        ) AS _top_users
        WHERE cnt > #min_orders_count#
    ),
    _price_changes AS (
        SELECT
            order_date, order_side, symbol,
            (
                SELECT price
                FROM #prices#
                WHERE TRUE
                    AND #prices#.symbol=_all_period.symbol
                    AND #prices#.order_side=_all_period.order_side
                    AND #prices#.order_date<=_all_period.order_date
                ORDER BY order_date DESC
                LIMIT 1
            ) - (
                SELECT price
                FROM #prices#
                WHERE TRUE
                    AND #prices#.symbol=_all_period.symbol
                    AND #prices#.order_side=_all_period.order_side
                    AND #prices#.order_date<_all_period.order_date
                ORDER BY order_date DESC
                LIMIT 1
            ) AS price_change
        FROM
            (
                    (
                        SELECT order_date
                        FROM generate_series('#min_order_date#'::timestamp with time zone,
                                             '#max_order_date#'::timestamp with time zone,
                                             '1 minute') AS _period(order_date)
                    ) AS _dates
                CROSS JOIN
                    (
                        SELECT DISTINCT symbol
                        FROM #prices#
                    ) AS _symbols
                CROSS JOIN
                    (
                        SELECT DISTINCT order_side
                        FROM #prices#
                    ) AS _order_sides
            ) AS _all_period
    )

SELECT
    user_name,
    symbol,
    buys_count,
    sells_count,
    avg_buy_amount,
    avg_sell_amount,
    total_profit,
    sh,
    active_price_change_corr_bin,
    active_price_change_corr_abs,
    total_price_change_corr_bin,
    total_price_change_corr_abs
FROM
        _selected_users
    NATURAL JOIN
        (
            SELECT user_name, symbol, count(*) AS buys_count
            FROM #orders_norm#
            WHERE #orders_norm#.order_side='buy'
                AND order_date >= '#min_order_date#'::timestamp with time zone
                AND order_date <= '#max_order_date#'::timestamp with time zone
            GROUP BY user_name, symbol
        ) AS _buys_count

    NATURAL JOIN
        (
            SELECT user_name, symbol, count(*) AS sells_count
            FROM #orders_norm#
            WHERE #orders_norm#.order_side='sell'
                AND order_date >= '#min_order_date#'::timestamp with time zone
                AND order_date <= '#max_order_date#'::timestamp with time zone
            GROUP BY user_name, symbol
        ) AS _sells_count

    NATURAL JOIN
        (
            SELECT user_name, symbol, avg(abs(balance_change_base)) AS avg_buy_amount
            FROM #orders_norm#
            WHERE #orders_norm#.order_side='buy'
                AND order_date >= '#min_order_date#'::timestamp with time zone
                AND order_date <= '#max_order_date#'::timestamp with time zone
            GROUP BY user_name, symbol
        ) AS _avg_buy_amount

    NATURAL JOIN
        (
            SELECT user_name, symbol, avg(abs(balance_change_base)) AS avg_sell_amount
            FROM #orders_norm#
            WHERE #orders_norm#.order_side='sell'
                AND order_date >= '#min_order_date#'::timestamp with time zone
                AND order_date <= '#max_order_date#'::timestamp with time zone
            GROUP BY user_name, symbol
        ) AS _avg_sell_amount

    NATURAL JOIN
        (
            SELECT user_name, symbol, sum(balance_change_total) AS total_profit
            FROM #orders_norm#
            WHERE TRUE
                AND order_date >= '#min_order_date#'::timestamp with time zone
                AND order_date <= '#max_order_date#'::timestamp with time zone
            GROUP BY user_name, symbol
        ) AS _total_profit

    NATURAL JOIN
        (
            SELECT user_name, symbol,
                CASE stddev_samp(balance_change_total) > 1.0E-4
                WHEN TRUE THEN avg(balance_change_total)/stddev_samp(balance_change_total)
                ELSE NULL END AS sh
            FROM #orders_norm#
            WHERE TRUE
                AND order_date >= '#min_order_date#'::timestamp with time zone
                AND order_date <= '#max_order_date#'::timestamp with time zone
            GROUP BY user_name, symbol
        ) AS _sh
    
    NATURAL JOIN
        (
            SELECT
                user_name, symbol,
                corr(price_change, balance_change_base) AS active_price_change_corr_bin
            FROM
                    _price_changes
                NATURAL INNER JOIN
                    (
                        SELECT
                            order_date,
                            order_side,
                            user_name,
                            symbol,
                            CASE
                                WHEN balance_change_base > 0 THEN 1
                                WHEN balance_change_base < 0 THEN -1
                                ELSE 0
                            END AS balance_change_base
                        FROM #orders_norm#
                    ) AS _orders_norm
            GROUP BY user_name, symbol
        ) AS active_price_change_corr_bin

    NATURAL JOIN
        (
            SELECT
                user_name, symbol,
                corr(price_change, balance_change_base) AS active_price_change_corr_abs
            FROM
                    _price_changes
                NATURAL INNER JOIN
                    (
                        SELECT
                            order_date,
                            order_side,
                            user_name,
                            symbol,
                            balance_change_base
                        FROM #orders_norm#
                    ) AS _orders_norm
            GROUP BY user_name, symbol
        ) AS active_price_change_corr_abs

    NATURAL JOIN
        (
            SELECT
                user_name, symbol,
                corr(price_change, (SELECT COALESCE(balance_change_base, 0))) AS total_price_change_corr_bin
            FROM
                    _price_changes
                CROSS JOIN
                    (
                        SELECT DISTINCT user_name
                        FROM _selected_users
                    ) AS _users
                NATURAL LEFT JOIN
                    (
                        SELECT
                            order_date,
                            order_side,
                            user_name,
                            symbol,
                            CASE
                                WHEN balance_change_base > 0 THEN 1
                                WHEN balance_change_base < 0 THEN -1
                                ELSE 0
                            END AS balance_change_base
                        FROM #orders_norm#
                    ) AS _orders_norm
            GROUP BY user_name, symbol
        ) AS total_price_change_corr_bin

    NATURAL JOIN
        (
            SELECT
                user_name, symbol,
                corr(price_change, (SELECT COALESCE(balance_change_base, 0))) AS total_price_change_corr_abs
            FROM
                    _price_changes
                CROSS JOIN
                    (
                        SELECT DISTINCT user_name
                        FROM _selected_users
                    ) AS _users
                NATURAL LEFT JOIN
                    (
                        SELECT
                            order_date,
                            order_side,
                            symbol,
                            user_name,
                            balance_change_base
                        FROM #orders_norm#
                    ) AS _orders_norm
            GROUP BY user_name, symbol
        ) AS total_price_change_corr_abs

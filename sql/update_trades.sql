UPDATE #trades#
SET
    balance_change_trade = CASE order_side WHEN 'sell' THEN -amount_trade ELSE amount_trade END,
    balance_change_base = CASE order_side WHEN 'sell' THEN amount_base ELSE -amount_base END
WHERE balance_change_trade IS NULL OR balance_change_base IS NULL
;

INSERT INTO #balances#
SELECT
    user_name, symbol, order_date, order_id,
    sum(balance_change_trade) OVER(PARTITION BY user_name, symbol ORDER BY order_date) AS balance_trade,
    sum(balance_change_base) OVER(PARTITION BY user_name, symbol ORDER BY order_date) AS balance_base
FROM #trades#
ON CONFLICT DO NOTHING
;

UPDATE #trades#
SET
    balance_trade = (
        SELECT balance_trade
        FROM #balances#
        WHERE
                #trades#.user_name=#balances#.user_name
            AND #trades#.symbol=#balances#.symbol
            AND #trades#.order_date=#balances#.order_date
            AND #trades#.order_id=#balances#.order_id
        LIMIT 1
    ),
    balance_base = (
        SELECT balance_base
        FROM #balances#
        WHERE
                #trades#.user_name=#balances#.user_name
            AND #trades#.symbol=#balances#.symbol
            AND #trades#.order_date=#balances#.order_date
            AND #trades#.order_id=#balances#.order_id
        LIMIT 1
    )
WHERE balance_base IS NULL OR balance_trade IS NULL
;
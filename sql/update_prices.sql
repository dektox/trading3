INSERT INTO #prices#(order_date, order_side, symbol, price)
SELECT
    order_date,
    order_side,
    symbol,
    (sum(amount_base)::double precision/sum(amount_trade)::double precision) AS price
FROM
    (
        SELECT
            date_trunc('minute', order_date) AS order_date,
            order_side,
            symbol,
            amount_base,
            amount_trade
        FROM
            #orders#
    ) AS _orders
GROUP BY
    order_date,
    order_side,
    symbol
ON CONFLICT DO NOTHING
;

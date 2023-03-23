SELECT
    id,
    TIME sold_time,
    datetime,
    orderid AS sell_order_id,
    symbol,
    price,
    amount,
    fee sell_fee,
    COALESCE(LAG(fee) over w, 0) AS prev_sell_fee,
    COALESCE(LAG(amount) over w, 0) AS prev_sold_qty,
    price * amount proceeds,
    SUM(amount) over w AS cum_sold_qty,
    COALESCE(SUM(amount) over prevw, 0) AS cum_prev_sold_qty,
    cost -- ,
    -- SUM(cost) over w AS total_cost -- ,
    -- COALESCE(SUM(cost) over prevw, 0) AS prev_total_cost
FROM
    PUBLIC.orderstable
WHERE
    symbol = 'NANO'
    AND side = 'sell' window w AS (
        PARTITION BY symbol
        ORDER BY
            TIME
    ),
    prevw AS (
        PARTITION BY symbol
        ORDER BY
            TIME rows BETWEEN unbounded preceding
            AND 1 preceding
    )
ORDER BY
    TIME DESC

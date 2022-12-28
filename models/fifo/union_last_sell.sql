WITH union_sell_order AS (
    SELECT
        symbol,
        buy_order_id,
        bought_time,
        bought_price,
        bought_qty,
        -- bought_cost,
        sell_order_id,
        sold_price,
        sold_qty,
        proceeds,
        sold_datetime,
        sold_time
    FROM
        {{ ref('bought_cost') }}
    WHERE
        sold_time != 0
    UNION
    SELECT
        symbol,
        buy_order_id,
        bought_time,
        bought_price,
        bought_qty,
        -- bought_cost,
        sell_order_id,
        sold_price,
        sold_qty,
        proceeds,
        sold_datetime,
        sold_time
    FROM
        {{ ref('last_buy') }}
    ORDER BY
        bought_time,
        sold_time
)
SELECT
    symbol,
    buy_order_id,
    bought_time date_acquire,
    bought_price,
    bought_qty,
    bought_price * sold_qty liquidiate_cost,
    sell_order_id,
    LAG(sell_order_id) over (
        PARTITION BY symbol
        ORDER BY
            bought_time
    ) AS prev_sell_order_id,
    sold_price,
    sold_qty,
    COALESCE(LAG(sold_qty) over (PARTITION BY symbol
ORDER BY
    sold_time), 0) AS prev_sold_qty,
    proceeds,
    sold_datetime date_sold,
    proceeds - (
        bought_price * sold_qty
    ) order_pnL,
    sold_time
FROM
    union_sell_order
ORDER BY
    bought_time,
    sold_time

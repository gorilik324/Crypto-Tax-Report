WITH o1 AS (
    SELECT
        *
    FROM
        {{ ref('union_last_sell') }}
    WHERE
        sell_order_id != prev_sell_order_id
)
SELECT
    symbol,
    buy_order_id,
    date_acquire,
    bought_price,
    bought_qty,
    liquidiate_cost,
    buy_fee,
    sell_order_id,
    sold_price,
    sell_fee,
    sold_qty,
    original_sold_qty,
    proceeds,
    date_sold,
    order_pnL
FROM
    o1
UNION
SELECT
    symbol,
    buy_order_id,
    date_acquire,
    bought_price,
    bought_qty,
    liquidiate_cost,
    buy_fee,
    sell_order_id,
    sold_price,
    sell_fee,
    sold_qty,
    original_sold_qty,
    proceeds,
    date_sold,
    order_pnL
FROM
    {{ ref('first_sell') }}
ORDER BY
    -- date_acquire,
    date_acquire,
    date_sold

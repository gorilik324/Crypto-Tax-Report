WITH buy1 AS (
    SELECT
        DISTINCT *
    FROM
        {{ ref('union_last_sell') }}
    WHERE
        cum_prev_bought_qty != 0
        AND cum_prev_bought_qty < cum_sold_qty
        AND cum_sold_qty < cum_bought_qty
),
buy2 AS (
    SELECT
        MIN(sold_time) sell_time,
        buy_order_id id
    FROM
        buy1
    GROUP BY
        buy_order_id
),
buy3 AS (
    SELECT
        *
    FROM
        buy1
        JOIN buy2
        ON buy2.sell_time = buy1.sold_time
        AND buy2.id = buy1.buy_order_id
    ORDER BY
        date_acquire,
        sold_time
),
buy4 AS (
    SELECT
        DISTINCT *
    FROM
        buy3
    WHERE
        (
            sold_qty - prev_sold_qty
        ) > 0
),
buy5 AS (
    SELECT
        buy_order_id boi,
        sell_order_id soi,
        order_pnL oPL
    FROM
        buy4
),
buy6 AS (
    SELECT
        *
    FROM
        {{ ref('union_last_sell') }}
        uls
        LEFT JOIN buy5
        ON uls.buy_order_id = buy5.boi
        AND uls.sell_order_id = buy5.soi
    WHERE
        buy5.boi IS NULL
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
    buy6
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
    {{ ref('first_sell_pos_adj') }}
ORDER BY
    -- date_acquire,
    date_acquire,
    date_sold

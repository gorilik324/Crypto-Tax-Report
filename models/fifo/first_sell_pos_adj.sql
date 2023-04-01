WITH buy1 AS (
    SELECT
        DISTINCT *
    FROM
        {{ ref('bought_cost_final') }}
    WHERE
        cum_prev_bought_qty != 0
        AND cum_prev_sold_qty != 0 -- AND follow_bought_qty != 0
),
buy2 AS (
    SELECT
        MIN(buy_time) buy_time1
    FROM
        buy1
    GROUP BY
        sell_order_id
),
buy3 AS (
    SELECT
        DISTINCT *
    FROM
        buy1
        JOIN buy2
        ON buy2.buy_time1 = buy1.buy_time
    ORDER BY
        bought_time,
        sold_time
),
buy4 AS (
    SELECT
        -- buy orders
        DISTINCT symbol,
        cum_bought_qty,
        -- follow_sell_order_id,
        prev_buy_time buy_time,
        prev_bought_time date_acquire,
        prev_buy_fee buy_fee,
        prev_buy_order_id buy_order_id,
        prev_bought_price bought_price,
        prev_bought_qty bought_qty,
        prev_bought_cost bought_cost,
        (
            cum_prev_bought_qty - cum_prev_sold_qty
        ) * prev_bought_price liquidiate_cost,
        -- sell orders
        sell_order_id,
        cum_sold_qty,
        sold_price,
        sold_qty original_sold_qty,
        (
            cum_prev_bought_qty - cum_prev_sold_qty
        ) sold_qty,
        (
            cum_prev_bought_qty - cum_prev_sold_qty
        ) * sold_price proceeds,
        sold_datetime date_sold,
        sell_fee,
        sold_time,
        cum_prev_bought_qty,
        cum_prev_sold_qty,
        prev_sold_qty
    FROM
        buy3
    WHERE
        cum_prev_bought_qty - cum_prev_sold_qty > 0
)
SELECT
    DISTINCT symbol,
    buy_order_id,
    date_acquire,
    bought_price,
    bought_qty,
    liquidiate_cost,
    buy_fee,
    sold_time,
    sell_order_id,
    sold_price,
    sell_fee,
    cum_prev_bought_qty,
    sold_qty,
    proceeds,
    original_sold_qty,
    cum_sold_qty,
    cum_bought_qty,
    -- CASE
    --     WHEN (
    --         sold_qty - prev_sold_qty
    --     ) > bought_qty THEN bought_qty
    --     WHEN sell_order_id != follow_sell_order_id THEN (
    --         sold_qty - prev_sold_qty
    --     )
    --     ELSE bought_qty
    -- END sold_qty,
    prev_sold_qty,
    -- CASE
    --     WHEN (
    --         sold_qty - prev_sold_qty
    --     ) > bought_qty THEN bought_qty * sold_price
    --     ELSE (
    --         sold_qty - prev_sold_qty
    --     ) * sold_price
    -- END proceeds,
    proceeds - liquidiate_cost AS order_pnL,
    date_sold
FROM
    buy4
ORDER BY
    --     -- buy_order_id,
    date_acquire,
    date_sold -- buy_order_id -- sold_time -- )

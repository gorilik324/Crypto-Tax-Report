WITH buy1 AS (
    SELECT
        DISTINCT *
    FROM
        {{ ref('union_last_sell') }}
        -- WHERE
        --     cum_prev_bought_qty != 0
        --     AND cum_prev_bought_qty < cum_sold_qty
        --     AND cum_sold_qty < cum_bought_qty
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
        DISTINCT *
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
        cum_prev_bought_qty != 0
        AND cum_prev_bought_qty < cum_sold_qty
        AND cum_sold_qty < cum_bought_qty
        AND (
            sold_qty - prev_sold_qty
        ) > 0 -- AND sell_order_id != follow_sell_order_id
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
    -- sold_qty,
    original_sold_qty,
    cum_sold_qty,
    cum_bought_qty,
    CASE
        WHEN (
            sold_qty - prev_sold_qty
        ) > bought_qty THEN bought_qty
        WHEN sell_order_id != follow_sell_order_id THEN (
            sold_qty - prev_sold_qty
        )
        ELSE bought_qty
    END sold_qty,
    prev_sold_qty,
    CASE
        WHEN (
            sold_qty - prev_sold_qty
        ) > bought_qty THEN bought_qty * sold_price
        ELSE (
            sold_qty - prev_sold_qty
        ) * sold_price
    END proceeds,
    CASE
        WHEN (
            sold_qty - prev_sold_qty
        ) > bought_qty THEN (
            bought_qty * sold_price
        ) - (
            bought_price * (
                bought_qty
            )
        )
        ELSE ((sold_qty - prev_sold_qty) * sold_price) - (
            bought_price * (
                sold_qty - prev_sold_qty
            )
        )
    END order_pnL,
    date_sold
FROM
    buy4
WHERE
    (
        sold_qty - prev_sold_qty
    ) >= 0
ORDER BY
    --     -- buy_order_id,
    date_acquire,
    date_sold -- buy_order_id -- sold_time -- )

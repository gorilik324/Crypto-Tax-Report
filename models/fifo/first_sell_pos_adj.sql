WITH buy1 AS (
    SELECT
        DISTINCT *
    FROM
        -- {{ ref('bought_cost_final') }}
        {{ ref('union_last_sell') }}
        -- {{ ref('bought_cost') }}
    WHERE
        cum_prev_bought_qty != 0
        AND cum_prev_bought_qty < cum_sold_qty
        AND cum_sold_qty < cum_bought_qty
),
buy2 AS (
    SELECT
        MIN(sold_time) sell_time
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
        ELSE (
            sold_qty - prev_sold_qty
        )
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

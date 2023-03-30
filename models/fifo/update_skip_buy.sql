WITH buy1 AS (
    SELECT
        DISTINCT *
    FROM
        {{ ref('bought_cost_final') }}
        -- {{ ref('cum_bought_cost') }}
    WHERE
        cum_prev_bought_qty < cum_sold_qty
        AND cum_sold_qty > cum_bought_qty
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
        *
    FROM
        buy1
        JOIN buy2
        ON buy2.sell_time = buy1.sold_time
    ORDER BY
        bought_time,
        sold_time
),
buy4 AS (
    SELECT
        *
    FROM
        buy3
    WHERE
        (
            cum_prev_bought_qty - cum_prev_sold_qty
        ) > 0
),
buy5 AS (
    SELECT
        *,
        COALESCE(
            (LEAD(sell_order_id, 2) over (PARTITION BY symbol
            ORDER BY
                bought_time)),
                'a'
        ) follow_sell_order_id
    FROM
        buy4
)
SELECT
    DISTINCT symbol,
    prev_buy_time buy_time,
    prev_bought_cost bought_cost,
    prev_buy_order_id buy_order_id,
    prev_bought_time bought_time,
    prev_bought_price bought_price,
    prev_bought_qty bought_qty,
    ROUND((prev_bought_price * prev_sold_qty), 4) liquidiate_cost,
    prev_buy_fee buy_fee,
    sold_time,
    sell_order_id,
    sold_price,
    sell_fee,
    cum_prev_bought_qty,
    sold_datetime,
    cum_prev_sold_qty,
    -- sold_qty,
    -- original_sold_qty,
    cum_sold_qty,
    cum_bought_qty,
    CASE
        WHEN follow_sell_order_id = sell_order_id THEN bought_qty
        ELSE (
            cum_prev_bought_qty - cum_prev_sold_qty
        )
    END sold_qty,
    prev_sold_qty,
    (
        cum_prev_bought_qty - cum_prev_sold_qty
    ) * sold_price proceeds,
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
    follow_bought_qty,
    follow_sell_order_id -- ,date_sold
FROM
    buy5
WHERE
    (
        sold_qty - prev_sold_qty
    ) < 0
    AND sell_order_id != follow_sell_order_id -- AND follow_sell_order_id != 'a' --     AND original_sold_qty != cum_prev_bought_qty
ORDER BY
    --     -- buy_order_id,
    bought_time,
    sold_time -- buy_order_id -- sold_time -- )

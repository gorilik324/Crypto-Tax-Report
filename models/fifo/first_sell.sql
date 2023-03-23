WITH first_sell_order AS (
    SELECT
        MIN(sold_time) sell_time,
        buy_order_id
    FROM
        {{ ref('union_last_sell') }}
    GROUP BY
        buy_order_id
),
first_sell_order_with_details AS (
    SELECT
        uls.*
    FROM
        {{ ref('union_last_sell') }}
        uls
        JOIN first_sell_order
        ON first_sell_order.sell_time = uls.sold_time
        AND first_sell_order.buy_order_id = uls.buy_order_id
    WHERE
        prev_sell_order_id != 'a'
    ORDER BY
        date_acquire,
        date_sold
)
SELECT
    symbol,
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
    -- sold_qty,
    original_sold_qty,
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
    --
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
    first_sell_order_with_details
WHERE
    (
        sold_qty - prev_sold_qty
    ) >= 0
ORDER BY
    --     -- buy_order_id,
    date_acquire,
    date_sold -- buy_order_id -- sold_time -- )

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
    ORDER BY
        date_acquire
)
SELECT
    symbol,
    buy_order_id,
    date_acquire,
    -- bought_time,
    bought_price,
    bought_qty,
    -- cum_prev_bought_qty,
    -- cum_bought_qty,
    liquidiate_cost,
    -- bought_cost,
    -- total_cost,
    -- prev_total_cost,
    --sell orders
    sell_order_id,
    sold_price,
    (
        sold_qty - prev_sold_qty
    ) sold_qty,
    prev_sold_qty,
    (
        sold_qty - prev_sold_qty
    ) * sold_price proceeds,
    ((sold_qty - prev_sold_qty) * sold_price) - (
        bought_price * (
            sold_qty - prev_sold_qty
        )
    ) order_pnL,
    date_sold,
    sold_time
FROM
    first_sell_order_with_details -- )

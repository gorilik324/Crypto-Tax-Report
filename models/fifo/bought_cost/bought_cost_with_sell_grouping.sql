WITH group1 AS (
    SELECT
        -- buy orders
        symbol,
        buy_order_id,
        buy_time,
        bought_time,
        bought_price,
        bought_qty,
        bought_cost,
        buy_fee,
        prev_buy_time,
        prev_bought_time,
        prev_buy_fee,
        prev_buy_order_id,
        prev_bought_price,
        prev_bought_qty,
        prev_bought_cost,
        cum_prev_bought_qty,
        --sell orders
        sold_time,
        sold_price,
        sell_fee,
        sold_qty,
        proceeds,
        sell_order_id,
        sold_datetime,
        cum_prev_sold_qty,
        SUM(pre_group) over (
            PARTITION BY symbol
            ORDER BY
                bought_time
        ) group1
    FROM
        {{ ref('bought_cost') }}
    ORDER BY
        bought_time,
        sold_time
)
SELECT
    *,
    COALESCE(LAG(group1, 1) over (
ORDER BY
    bought_time), -1) group2
FROM
    group1
ORDER BY
    bought_time,
    sold_time

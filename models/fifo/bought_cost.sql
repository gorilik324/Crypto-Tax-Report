WITH bc AS (
    SELECT
        -- buy orders
        symbol,
        buy_order_id,
        prev_buytime,
        bought_time,
        prev_bought_price,
        bought_price,
        prev_bought_qty,
        bought_qty,
        cum_prev_bought_qty,
        cum_bought_qty,
        prev_bought_cost,
        bought_cost,
        prev_buy_order_id,
        --sell orders
        COALESCE(
            sold_time,
            0
        ) sold_time,
        COALESCE(
            sold_price,
            0
        ) sold_price,
        COALESCE(
            sold_qty,
            0
        ) sold_qty,
        COALESCE(
            cum_prev_sold_qty,
            0
        ) cum_prev_sold_qty,
        COALESCE(
            cum_sold_qty,
            0
        ) cum_sold_qty,
        COALESCE(
            proceeds,
            0
        ) proceeds,
        COALESCE(
            sell_order_id,
            'aaa'
        ) sell_order_id,
        COALESCE(
            sold_datetime,
            'bbb'
        ) sold_datetime
    FROM
        {{ ref('cum_bought_cost') }}
    ORDER BY
        bought_time
)
SELECT
    *,
    LEAD(sell_order_id) over (
        PARTITION BY symbol
        ORDER BY
            bought_time
    ) AS former_sell_order_id,
    LEAD(sold_time) over (
        PARTITION BY symbol
        ORDER BY
            bought_time
    ) AS former_sold_time,
    LEAD(sold_price) over (
        PARTITION BY symbol
        ORDER BY
            bought_time
    ) AS former_sold_price,
    LEAD(sold_qty) over (
        PARTITION BY symbol
        ORDER BY
            bought_time
    ) AS former_sold_qty,
    LEAD(cum_prev_sold_qty) over (
        PARTITION BY symbol
        ORDER BY
            bought_time
    ) AS former_cum_prev_sold_qty,
    LEAD(cum_sold_qty) over (
        PARTITION BY symbol
        ORDER BY
            bought_time
    ) AS former_cum_sold_qty,
    LEAD(sold_datetime) over (
        PARTITION BY symbol
        ORDER BY
            bought_time
    ) AS former_sold_datetime
FROM
    bc
ORDER BY
    bought_time -- WITH cte AS (
    --     SELECT
    --         *
    --     FROM
    --         {{ ref('cum_bought_cost') }} cbc
    --     WHERE sell_order_id IS not NULL
    --     UNION ALL
    --     SELECT
    --         e.staff_id,
    --         e.first_name,
    --         e.manager_id
    --     FROM
    --         sales.staffs e
    --         INNER JOIN cte_org o
    --             ON o.staff_id = e.manager_id
    -- )
    -- SELECT * FROM cte;

WITH buy1 AS (
    SELECT
        DISTINCT *
    FROM
        {{ ref('bought_cost_final') }}
    WHERE
        cum_prev_bought_qty != 0 -- AND follow_bought_qty != 0
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
        *
    FROM
        buy1
        JOIN buy2
        ON buy2.sell_time = buy1.sold_time
    ORDER BY
        bought_time,
        sold_time
)
SELECT
    -- buy orders
    DISTINCT symbol,
    cum_bought_qty,
    buy_time,
    bought_time,
    buy_fee,
    buy_order_id,
    bought_price,
    bought_qty,
    bought_cost,
    -- sell orders
    sell_order_id,
    cum_sold_qty,
    sold_price,
    sold_qty original_sold_qty,
    (
        cum_sold_qty - cum_prev_bought_qty
    ) sold_qty,
    (
        cum_sold_qty - cum_prev_bought_qty
    ) * sold_price proceeds,
    sold_datetime,
    sell_fee,
    sold_time,
    cum_prev_bought_qty,
    cum_prev_sold_qty,
    CASE
        WHEN sold_qty = 0 THEN 88
        ELSE 9999
    END bcf -- cum_bought_qty,
    -- total_cost,
    -- prev_total_cost,
    -- sold_qty original_sold_qty,
    -- cum_sold_qty,
    -- cum_proceeds,
FROM
    buy3
ORDER BY
    bought_time,
    sold_time -- WHERE
    --     (
    --         cum_prev_bought_qty - cum_prev_sold_qty
    --     ) > 0 -- WHERE
    --     sold_time != 0
    -- UNION
    -- SELECT
    --     DISTINCT symbol,
    --     prev_buy_order_id buy_order_id,
    --     prev_buytime bought_time,
    --     prev_bought_price bought_price,
    --     prev_bought_qty bought_qty,
    --     prev_buy_fee buy_fee,
    --     former_sold_time sold_time,
    --     former_sold_price sold_price,
    --     former_sold_qty original_sold_qty,
    --     (
    --         cum_prev_bought_qty - former_cum_prev_sold_qty
    --     ) sold_qty,
    --     former_sell_fee sell_fee,
    --     -- former_cum_prev_sold_qty cum_prev_sold_qty,
    --     -- cum_sold_qty,
    --     (
    --         cum_prev_bought_qty - former_cum_prev_sold_qty
    --     ) * former_sold_price proceeds,
    --     -- cum_proceeds,
    --     former_sell_order_id sell_order_id,
    --     former_sold_datetime sold_datetime
    -- FROM
    --     first_sell_order_exclude_first_order
    -- WHERE
    --     sold_time = 0
    -- ORDER BY
    --     bought_time -- UNION
    --     -- SELECT
    --     --     symbol,
    --     --     prev_buy_order_id buy_order_id,
    --     --     prev_buytime bought_time,
    --     --     prev_bought_price bought_price,
    --     --     prev_bought_qty bought_qty,
    --     --     prev_buy_fee buy_fee,
    --     --     sold_time,
    --     --     sold_price,
    --     --     sold_qty original_sold_qty,
    --     --     (
    --     --         cum_prev_bought_qty - cum_prev_sold_qty
    --     --     ) sold_qty,
    --     --     cum_prev_sold_qty,
    --     --     sell_fee -- cum_sold_qty,
    --     --     (
    --     --         cum_prev_bought_qty - cum_prev_sold_qty
    --     --     ) * sold_price proceeds,
    --     --     -- cum_proceeds,
    --     --     sell_order_id,
    --     --     sold_datetime
    --     -- FROM
    --     --     {{ ref('cum_bought_cost') }}
    --     -- WHERE
    --     --     sold_time IS NULL
    --     -- ORDER BY
    --     --     bought_time -- cum_prev_bought_qty,
    --     -- cum_bought_qty,
    --     -- prev_bought_cost bought_cost,
    --     -- total_cost,
    --     -- prev_total_cost,
    --     --sell orders

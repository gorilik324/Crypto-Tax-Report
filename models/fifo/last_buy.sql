WITH first_sell_order AS (
    SELECT
        MIN(sold_time) sell_time
    FROM
        -- {{ ref('cum_bought_cost') }}
        {{ ref('bought_cost') }}
    GROUP BY
        buy_order_id
),
first_sell_order_with_details AS (
    SELECT
        cbc.*
    FROM
        -- {{ ref('cum_bought_cost') }}
        {{ ref('bought_cost') }}
        cbc
        JOIN first_sell_order
        ON first_sell_order.sell_time = cbc.sold_time
    ORDER BY
        bought_time
),
first_sell_order_exclude_first_order AS (
    SELECT
        *
    FROM
        first_sell_order_with_details
    WHERE
        prev_bought_price != 0
)
SELECT
    DISTINCT symbol,
    prev_buy_order_id buy_order_id,
    prev_buytime bought_time,
    prev_bought_price bought_price,
    prev_bought_qty bought_qty,
    -- cum_prev_bought_qty,
    -- cum_bought_qty,
    -- prev_bought_cost bought_cost,
    -- total_cost,
    -- prev_total_cost,
    --sell orders
    sold_time,
    sold_price,
    (
        cum_prev_bought_qty - cum_prev_sold_qty
    ) sold_qty,
    -- cum_prev_sold_qty,
    -- cum_sold_qty,
    (
        cum_prev_bought_qty - cum_prev_sold_qty
    ) * sold_price proceeds,
    -- cum_proceeds,
    sell_order_id,
    sold_datetime
FROM
    first_sell_order_exclude_first_order
WHERE
    sold_time != 0
UNION
SELECT
    DISTINCT symbol,
    prev_buy_order_id buy_order_id,
    prev_buytime bought_time,
    prev_bought_price bought_price,
    prev_bought_qty bought_qty,
    former_sold_time sold_time,
    former_sold_price sold_price,
    (
        cum_prev_bought_qty - former_cum_prev_sold_qty
    ) sold_qty,
    -- former_cum_prev_sold_qty cum_prev_sold_qty,
    -- cum_sold_qty,
    (
        cum_prev_bought_qty - former_cum_prev_sold_qty
    ) * former_sold_price proceeds,
    -- cum_proceeds,
    former_sell_order_id sell_order_id,
    former_sold_datetime sold_datetime
FROM
    first_sell_order_exclude_first_order
WHERE
    sold_time = 0
ORDER BY
    bought_time -- UNION
    -- SELECT
    --     symbol,
    --     prev_buy_order_id buy_order_id,
    --     prev_buytime bought_time,
    --     prev_bought_price bought_price,
    --     prev_bought_qty bought_qty,
    --     -- cum_prev_bought_qty,
    --     -- cum_bought_qty,
    --     -- prev_bought_cost bought_cost,
    --     -- total_cost,
    --     -- prev_total_cost,
    --     --sell orders
    --     sold_time,
    --     sold_price,
    --     (
    --         cum_prev_bought_qty - cum_prev_sold_qty
    --     ) sold_qty,
    --     cum_prev_sold_qty,
    --     -- cum_sold_qty,
    --     (
    --         cum_prev_bought_qty - cum_prev_sold_qty
    --     ) * sold_price proceeds,
    --     -- cum_proceeds,
    --     sell_order_id,
    --     sold_datetime
    -- FROM
    --     {{ ref('cum_bought_cost') }}
    -- WHERE
    --     sold_time IS NULL
    -- ORDER BY
    --     bought_time

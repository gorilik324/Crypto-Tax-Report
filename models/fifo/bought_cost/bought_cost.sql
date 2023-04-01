-- WITH bc AS (
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
    follow_bought_qty,
    --sell orders
    COALESCE(
        cum_sold_qty,
        0
    ) cum_sold_qty,
    cum_bought_qty,
    COALESCE(
        sold_time,
        0
    ) sold_time,
    COALESCE(
        sold_price,
        0
    ) sold_price,
    COALESCE(
        sell_fee,
        0
    ) sell_fee,
    COALESCE(
        sold_qty,
        0
    ) sold_qty,
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
    ) sold_datetime,
    COALESCE(
        cum_prev_sold_qty,
        0
    ) cum_prev_sold_qty,
    COALESCE(
        prev_sold_qty,
        0
    ) prev_sold_qty,
    -- Grouping sell orders
    CASE
        WHEN sold_price > 0 THEN 1
        ELSE 0
    END pre_group
FROM
    {{ ref('cum_bought_cost') }}
ORDER BY
    bought_time,
    sold_time

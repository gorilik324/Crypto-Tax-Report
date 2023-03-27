SELECT
    -- buy orders
    symbol,
    buy_order_id,
    buy_time,
    bought_time,
    bought_price,
    bought_qty,
    buy_fee,
    bought_cost,
    cum_prev_bought_qty,
    cum_bought_qty,
    prev_buy_time,
    prev_bought_time,
    prev_buy_fee,
    prev_buy_order_id,
    prev_bought_price,
    prev_bought_qty,
    prev_bought_cost,
    -- sell orders
    sold_time,
    sold_price,
    sold_qty,
    proceeds,
    sell_fee,
    sell_order_id,
    sold_qty original_sold_qty,
    sold_datetime,
    cum_prev_sold_qty,
    cum_sold_qty,
    follow_bought_qty
FROM
    {{ ref('bought_cost_final') }}
UNION
SELECT
    symbol,
    buy_order_id,
    buy_time,
    bought_time,
    bought_price,
    bought_qty,
    buy_fee,
    bought_cost,
    cum_prev_bought_qty,
    cum_bought_qty,
    buy_time prev_buy_time,
    bought_time prev_bought_time,
    buy_fee prev_buy_fee,
    buy_order_id prev_buy_order_id,
    bought_price prev_bought_price,
    bought_qty prev_bought_qty,
    bought_cost prev_bought_cost,
    sold_time,
    sold_price,
    sold_qty,
    proceeds,
    sell_fee,
    sell_order_id,
    sold_qty original_sold_qty,
    sold_datetime,
    cum_prev_sold_qty,
    cum_sold_qty,
    follow_bought_qty
FROM
    {{ ref('update_skip_buy') }}
ORDER BY
    bought_time,
    sold_time

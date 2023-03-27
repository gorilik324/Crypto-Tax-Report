WITH union_sell_order AS (
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
        cum_sold_qty
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
        original_sold_qty,
        sold_datetime,
        cum_prev_sold_qty,
        cum_sold_qty
    FROM
        {{ ref('last_buy_pos') }}
    ORDER BY
        bought_time,
        sold_time
)
SELECT
    symbol,
    buy_order_id,
    buy_time,
    bought_time date_acquire,
    bought_price,
    bought_qty,
    ROUND((bought_price * sold_qty), 4) liquidiate_cost,
    buy_fee,
    cum_sold_qty,
    cum_bought_qty,
    sold_time,
    sell_order_id,
    sold_price,
    sell_fee,
    sold_qty,
    original_sold_qty,
    COALESCE(LAG(sold_qty) over (PARTITION BY symbol
ORDER BY
    sold_time), 0) AS prev_sold_qty,
    ROUND(
        proceeds,
        4
    ) proceeds,
    sold_datetime date_sold,
    proceeds - (
        bought_price * sold_qty
    ) order_pnL,
    cum_prev_bought_qty,
    cum_prev_sold_qty,
    bought_cost,
    COALESCE(
        (LAG(sell_order_id) over (PARTITION BY symbol
        ORDER BY
            bought_time)),
            'a'
    ) prev_sell_order_id,
    prev_buy_time,
    prev_bought_time,
    prev_buy_fee,
    prev_buy_order_id,
    prev_bought_price,
    prev_bought_qty,
    prev_bought_cost
FROM
    union_sell_order
WHERE
    sold_qty > 0
ORDER BY
    bought_time,
    sold_time

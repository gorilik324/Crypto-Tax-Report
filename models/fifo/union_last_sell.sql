WITH buy1 AS (
    SELECT
        DISTINCT *
    FROM
        {{ ref('bought_cost_final') }}
    WHERE
        cum_prev_bought_qty != 0
        AND cum_prev_bought_qty < cum_sold_qty
        AND cum_sold_qty < cum_bought_qty
),
buy2 AS (
    SELECT
        MIN(sold_time) sell_time,
        buy_order_id id
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
        AND buy2.id = buy1.buy_order_id
    ORDER BY
        bought_time,
        sold_time
),
buy5 AS (
    SELECT
        buy_order_id boi,
        sell_order_id soi
    FROM
        buy3
),
buy6 AS (
    SELECT
        *
    FROM
        {{ ref('bought_cost_final') }}
        uls
        LEFT JOIN buy5
        ON uls.buy_order_id = buy5.boi
        AND uls.sell_order_id = buy5.soi
    WHERE
        buy5.boi IS NULL
),
union_sell_order AS (
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
        bcf
    FROM
        buy6
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
        cum_sold_qty,
        bcf
    FROM
        {{ ref('last_sell_pos') }}
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
    ROUND((bought_price * sold_qty), 8) liquidiate_cost,
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
    bought_time, sold_time), 0) AS prev_sold_qty,
    ROUND(
        proceeds,
        8
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
            bought_time, sold_time)),
            'a'
    ) prev_sell_order_id,
    COALESCE(
        (LEAD(sell_order_id) over (PARTITION BY symbol
        ORDER BY
            bought_time, sold_time)),
            'a'
    ) follow_sell_order_id,
    prev_buy_time,
    prev_bought_time,
    prev_buy_fee,
    prev_buy_order_id,
    prev_bought_price,
    prev_bought_qty,
    prev_bought_cost,
    sold_datetime,
    bcf
FROM
    union_sell_order
WHERE
    sold_qty > 0
ORDER BY
    bought_time,
    sold_time

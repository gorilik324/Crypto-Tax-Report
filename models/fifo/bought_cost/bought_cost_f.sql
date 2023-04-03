WITH cbcfinal AS (
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
        CASE
            WHEN cum_sold_qty != 0 THEN cum_sold_qty
            WHEN cum_sold_qty = 0 THEN LAST_VALUE(
                cum_sold_qty
            ) over(
                PARTITION BY group2,
                symbol
                ORDER BY
                    cum_sold_qty RANGE BETWEEN unbounded preceding
                    AND unbounded following
            )
        END cum_sold_qty,
        cum_bought_qty,
        CASE
            WHEN sold_time != 0 THEN sold_time
            WHEN sold_time = 0 THEN LAST_VALUE(
                sold_time
            ) over(
                PARTITION BY group2,
                symbol
                ORDER BY
                    sold_time RANGE BETWEEN unbounded preceding
                    AND unbounded following
            )
        END sold_time,
        CASE
            WHEN sold_price != 0 THEN sold_price
            WHEN sold_price = 0 THEN LAST_VALUE(
                sold_price
            ) over(
                PARTITION BY group2,
                symbol
                ORDER BY
                    sold_price RANGE BETWEEN unbounded preceding
                    AND unbounded following
            )
        END sold_price,
        CASE
            WHEN sell_fee != 0 THEN sell_fee
            WHEN sell_fee = 0 THEN LAST_VALUE(
                sell_fee
            ) over(
                PARTITION BY group2,
                symbol
                ORDER BY
                    sell_fee RANGE BETWEEN unbounded preceding
                    AND unbounded following
            )
        END sell_fee,
        CASE
            WHEN sold_qty != 0 THEN sold_qty
            WHEN sold_qty = 0 THEN LAST_VALUE(
                sold_qty
            ) over(
                PARTITION BY group2,
                symbol
                ORDER BY
                    sold_qty RANGE BETWEEN unbounded preceding
                    AND unbounded following
            )
        END sold_qty,
        CASE
            WHEN proceeds != 0 THEN proceeds
            WHEN proceeds = 0 THEN LAST_VALUE(
                proceeds
            ) over(
                PARTITION BY group2,
                symbol
                ORDER BY
                    proceeds RANGE BETWEEN unbounded preceding
                    AND unbounded following
            )
        END proceeds,
        CASE
            WHEN cum_prev_sold_qty != 0 THEN cum_prev_sold_qty
            WHEN cum_prev_sold_qty = 0 THEN LAST_VALUE(
                cum_prev_sold_qty
            ) over(
                PARTITION BY group2,
                symbol
                ORDER BY
                    cum_prev_sold_qty RANGE BETWEEN unbounded preceding
                    AND unbounded following
            )
        END cum_prev_sold_qty,
        CASE
            WHEN prev_sold_qty != 0 THEN prev_sold_qty
            WHEN prev_sold_qty = 0 THEN LAST_VALUE(
                prev_sold_qty
            ) over(
                PARTITION BY group2,
                symbol
                ORDER BY
                    prev_sold_qty RANGE BETWEEN unbounded preceding
                    AND unbounded following
            )
        END prev_sold_qty,
        CASE
            WHEN sell_order_id != 'aaa' THEN sell_order_id
            WHEN sell_order_id = 'aaa' THEN LAST_VALUE(
                sell_order_id
            ) over(
                PARTITION BY group2,
                symbol
                ORDER BY
                    bought_time RANGE BETWEEN unbounded preceding
                    AND unbounded following
            )
        END sell_order_id,
        CASE
            WHEN sold_datetime != 'bbb' THEN sold_datetime
            WHEN sold_datetime = 'bbb' THEN LAST_VALUE(
                sold_datetime
            ) over(
                PARTITION BY group2,
                symbol
                ORDER BY
                    bought_time RANGE BETWEEN unbounded preceding
                    AND unbounded following
            )
        END sold_datetime
    FROM
        {{ ref('bought_cost_with_sell_grouping') }}
        -- WHERE
        --     cum_prev_bought_qty > 0
    ORDER BY
        bought_time,
        sold_time
),
FINAL AS (
    SELECT
        *,
        COALESCE(
            (LAG(sell_order_id) over (PARTITION BY symbol
            ORDER BY
                bought_time, sold_time)),
                'a'
        ) prev_sell_order_id
    FROM
        cbcfinal
    ORDER BY
        bought_time,
        sold_time
),
final1 AS (
    SELECT
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
        follow_bought_qty,
        cum_prev_bought_qty,
        cum_sold_qty,
        cum_bought_qty,
        sold_time,
        sold_price,
        sell_fee,
        cum_prev_sold_qty,
        prev_sold_qty,
        sell_order_id,
        prev_sell_order_id,
        sold_datetime,
        CASE
            WHEN cum_sold_qty >= cum_bought_qty
            AND cum_prev_bought_qty < cum_sold_qty THEN bought_qty
            ELSE sold_qty
        END sold_qty,
        CASE
            WHEN cum_sold_qty >= cum_bought_qty
            AND cum_prev_bought_qty < cum_sold_qty THEN bought_qty * sold_price
            ELSE proceeds
        END proceeds,
        CASE
            WHEN (
                cum_prev_bought_qty != 0
                AND cum_prev_bought_qty < cum_sold_qty
                AND cum_sold_qty < cum_bought_qty
            ) THEN NULL
            ELSE 1
        END bcf
    FROM
        FINAL
    ORDER BY
        bought_time,
        sold_time
)
SELECT
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
    follow_bought_qty,
    cum_prev_bought_qty,
    cum_sold_qty,
    cum_bought_qty,
    sold_time,
    sold_price,
    sell_fee,
    cum_prev_sold_qty,
    prev_sold_qty,
    sell_order_id,
    prev_sell_order_id,
    sold_datetime,
    cum_sold_qty - cum_prev_sold_qty sold_qty,
    (
        cum_sold_qty - cum_prev_sold_qty
    ) * sold_price proceeds,
    bcf
FROM
    final1
WHERE
    cum_sold_qty = cum_bought_qty
    AND cum_prev_bought_qty < cum_sold_qty
    AND sell_order_id != prev_sell_order_id

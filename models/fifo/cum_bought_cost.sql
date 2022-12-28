-- WITH cbc AS (
SELECT
    -- buy orders
    tpos.symbol,
    tpos.buy_order_id,
    -- tpos.time buy_time,
    LAG(
        tpos.datetime
    ) over (
        PARTITION BY tpos.symbol
        ORDER BY
            tpos.time
    ) AS prev_buytime,
    tpos.datetime bought_time,
    tpos.prev_bought_price,
    tpos.price bought_price,
    tpos.prev_bought_qty,
    tpos.amount bought_qty,
    tpos.cum_prev_bought_qty,
    tpos.cum_bought_qty,
    tpos.prev_bought_cost,
    tpos.cost bought_cost,
    -- tpos.total_cost,
    -- tpos.prev_total_cost,
    tpos.prev_buy_order_id,
    --sell orders
    tneg.time sold_time,
    tneg.price sold_price,
    tneg.amount sold_qty,
    tneg.cum_prev_sold_qty,
    tneg.cum_sold_qty,
    tneg.proceeds,
    -- tneg.total_cost cum_proceeds,
    -- tneg.prev_total_cost,
    tneg.sell_order_id,
    tneg.datetime sold_datetime
FROM
    {{ ref('sell_order') }}
    tneg
    RIGHT JOIN {{ ref('buy_order') }}
    tpos
    ON (
        tneg.cum_sold_qty BETWEEN tpos.cum_prev_bought_qty
        AND tpos.cum_bought_qty
    )
    AND tpos.symbol = tneg.symbol -- )
    -- SELECT
    --     -- buy orders
    --     symbol,
    --     buy_order_id,
    --     prev_buytime,
    --     bought_time,
    --     prev_bought_price,
    --     bought_price,
    --     prev_bought_qty,
    --     bought_qty,
    --     cum_prev_bought_qty,
    --     cum_bought_qty,
    --     prev_bought_cost,
    --     bought_cost,
    --     prev_buy_order_id,
    --     --sell orders
    --     sold_time,
    --     COALESCE(
    --         sold_price,
    --         0
    --     ) sold_price,
    --     COALESCE(
    --         sold_qty,
    --         0
    --     ) sold_qty,
    --     COALESCE(
    --         cum_prev_sold_qty,
    --         0
    --     ) cum_prev_sold_qty,
    --     COALESCE(
    --         cum_sold_qty,
    --         0
    --     ) cum_sold_qty,
    --     COALESCE(
    --         proceeds,
    --         0
    --     ) proceeds,
    --     COALESCE(
    --         sell_order_id,
    --         'aaa'
    --     ) sell_order_id,
    --     COALESCE(
    --         sold_datetime,
    --         'bbb'
    --     ) sold_datetime
    -- FROM
    --     cbc
    -- ORDER BY
    --     bought_time

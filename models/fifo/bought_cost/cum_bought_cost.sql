-- WITH cbc AS (
SELECT
    -- buy orders
    tpos.symbol,
    tpos.buy_order_id,
    tpos.buy_time buy_time,
    tpos.bought_time,
    tpos.price bought_price,
    tpos.amount bought_qty,
    tpos.cum_bought_qty,
    tpos.cost bought_cost,
    tpos.buy_fee,
    tpos.follow_bought_qty,
    -- sell orders
    tneg.sold_time,
    tneg.price sold_price,
    tneg.sell_fee,
    tneg.amount sold_qty,
    tneg.cum_sold_qty,
    tneg.proceeds,
    tneg.sell_order_id,
    tneg.datetime sold_datetime,
    -- end
    tpos.prev_buy_time,
    LAG(
        tpos.bought_time
    ) over (
        PARTITION BY tpos.symbol
        ORDER BY
            tpos.buy_time,
            tneg.sold_time
    ) AS prev_bought_time,
    tpos.prev_buy_fee,
    tpos.prev_buy_order_id,
    tpos.prev_bought_price,
    tpos.prev_bought_qty,
    tpos.prev_bought_cost,
    tpos.cum_prev_bought_qty,
    -- tpos.total_cost,
    -- tpos.prev_total_cost,
    -- tpos.prev_buy_fee,
    --sell orders
    -- tneg.prev_sell_fee,
    tneg.cum_prev_sold_qty -- tneg.total_cost cum_proceeds,
    -- tneg.prev_total_cost,
FROM
    {{ ref('sell_order') }}
    tneg full
    OUTER JOIN {{ ref('buy_order') }}
    tpos
    ON (
        tpos.cum_prev_bought_qty < tneg.cum_sold_qty
        AND tneg.cum_sold_qty <= tpos.cum_bought_qty
    )
    AND tpos.symbol = tneg.symbol
ORDER BY
    buy_time,
    sold_time -- )
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

WITH sell_order AS (
    SELECT
        sell_order_id AS id,
        amount AS orig_qty
    FROM
        {{ ref('sell_order') }}
)
SELECT
    upd.symbol,
    upd.buy_order_id,
    upd.bought_qty,
    upd.bought_price,
    upd.liquidiate_cost,
    upd.sell_order_id,
    upd.sold_qty,
    upd.sold_price,
    upd.proceeds,
    upd.date_acquire,
    upd.buy_fee,
    upd.sell_fee,
    -- original_sold_qty,
    upd.date_sold,
    upd.order_pnL,
    sell_order.orig_qty
FROM
    {{ ref('update') }}
    upd
    JOIN sell_order
    ON upd.sell_order_id = sell_order.id
ORDER BY
    date_acquire,
    date_sold

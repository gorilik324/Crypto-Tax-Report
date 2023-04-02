SELECT
    symbol,
    buy_order_id,
    date_acquire,
    bought_price,
    bought_qty,
    liquidiate_cost,
    ROUND((sold_qty / bought_qty) * buy_fee, 5) AS buy_fee,
    sell_order_id,
    sold_price,
    sold_qty,
    ROUND((sold_qty / original_sold_qty) * sell_fee, 5) AS sell_fee,
    ROUND(
        proceeds,
        5
    ) proceeds,
    date_sold,
    ROUND(
        order_pnL,
        5
    ) order_pnL
FROM
    {{ ref('update') }}

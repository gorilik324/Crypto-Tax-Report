SELECT
    symbol,
    buy_order_id,
    date_acquire,
    bought_price,
    bought_qty,
    liquidiate_cost,
    ROUND((sold_qty / bought_qty) * buy_fee, 8) AS buy_fee,
    sell_order_id,
    sold_price,
    sold_qty,
    ROUND((sold_qty / orig_qty) * sell_fee, 8) AS sell_fee,
    ROUND(
        proceeds,
        8
    ) proceeds,
    date_sold,
    ROUND(
        order_pnL,
        8
    ) order_pnL
FROM
    {{ ref('union_original_sold_qty') }}

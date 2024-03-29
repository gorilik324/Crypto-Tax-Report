SELECT
    symbol,
    buy_order_id,
    sold_qty AS shares,
    date_acquire,
    date_sold,
    proceeds,
    liquidiate_cost bought_cost,
    buy_fee + sell_fee fee_cost,
    (
        order_pnL - buy_fee - sell_fee
    ) gain,
    sell_order_id
FROM
    {{ ref('updated_with_fee') }}

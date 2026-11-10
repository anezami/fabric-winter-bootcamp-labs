# Starter CSVs

These are hand-authored synthetic Zava DIY retail records. Names and email
addresses are fictional; addresses use the reserved `example.test` domain.
All rows are intentionally valid. Keep the uploaded files unchanged.

| Local file | Rows | OneLake destination |
|---|---|---|
| [initial/customers.csv](initial/customers.csv) | 6 | `Files/bootcamp/initial/customers.csv` |
| [initial/products.csv](initial/products.csv) | 4 | `Files/bootcamp/initial/products.csv` |
| [initial/stores.csv](initial/stores.csv) | 2 | `Files/bootcamp/initial/stores.csv` |
| [initial/orders.csv](initial/orders.csv) | 7 | `Files/bootcamp/initial/orders.csv` |
| [initial/order_lines.csv](initial/order_lines.csv) | 9 | `Files/bootcamp/initial/order_lines.csv` |
| [changes/customer_changes.csv](changes/customer_changes.csv) | 3 | `Files/bootcamp/changes/customer_changes.csv` |

Initial customers are effective from 1 January 2026. Sales span
10-16 October 2026. The customer events are effective on 2 November 2026:
C003 moves from Utrecht/Silver to Amsterdam/Gold, C007 is inserted, and C002
is deleted. Each customer has at most one business event in this batch.

The nine order lines total **16 units and 516.73 net sales**, after rounding
each line to two decimals. They produce seven date/store/channel groups.
Use the [specification](../../specs/bootcamp-lakehouse.spec.md) for all types,
history rules and acceptance checks.

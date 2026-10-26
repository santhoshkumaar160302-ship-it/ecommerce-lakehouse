"""Build tableau/sales_report.hyper (Tableau extract) from sales_report.csv: python make_extract.py <csv> <out.hyper>"""
import sys
from tableauhyperapi import *
csv, out = sys.argv[1], sys.argv[2]
cols = [("transaction_id",SqlType.big_int()),("seq_no",SqlType.big_int()),("transaction_date",SqlType.date()),
("year",SqlType.big_int()),("quarter",SqlType.text()),("month_name",SqlType.text()),("day_name",SqlType.text()),
("is_weekend",SqlType.big_int()),("customer_id",SqlType.text()),("country",SqlType.text()),("state",SqlType.text()),
("region",SqlType.text()),("product_id",SqlType.text()),("category_name",SqlType.text()),("brand_name",SqlType.text()),
("channel",SqlType.text()),("coupon_flag",SqlType.big_int()),("quantity",SqlType.big_int()),("gross_amount",SqlType.double()),
("discount_amount",SqlType.big_int()),("net_amount",SqlType.double()),("net_amount_inr",SqlType.big_int())]
t = TableDefinition(TableName("Extract","Extract"), [TableDefinition.Column(n,ty,NULLABLE) for n,ty in cols])
with HyperProcess(Telemetry.DO_NOT_SEND_USAGE_DATA_TO_TABLEAU) as hp:
    with Connection(hp.endpoint, out, CreateMode.CREATE_AND_REPLACE) as c:
        c.catalog.create_schema("Extract"); c.catalog.create_table(t)
        n = c.execute_command(f"COPY {t.table_name} FROM {escape_string_literal(csv)} WITH (format csv, header true, NULL '')")
        print(n, c.execute_scalar_query('SELECT SUM(net_amount_inr)/88.29 FROM "Extract"."Extract"'))

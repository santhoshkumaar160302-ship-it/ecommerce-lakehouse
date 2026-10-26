"""Generate the Tableau workbook (ecommerce_sales.twbx) from tableau/sales_report.csv.

The .twbx is a zip holding the workbook XML (.twb) plus the CSV, so it opens anywhere
without re-pointing the data source. Usage:  python tableau/build_workbook.py <sales_report.csv>
"""

import sys
import zipfile
from pathlib import Path
from xml.sax.saxutils import escape

FX_INR_PER_USD = 88.29  # same rate as the gold fact notebook
DS = "federated.ecom0sales"
CONN = "textscan.ecom0sales"
DATA_DIR = "Data/Datasources"
HYPER_PATH = "Data/Extracts/sales_report.hyper"

# (column, tableau datatype, role, type)
COLUMNS = [
    ("transaction_id", "integer", "dimension", "ordinal"),
    ("seq_no", "integer", "dimension", "ordinal"),
    ("transaction_date", "date", "dimension", "ordinal"),
    ("year", "integer", "dimension", "ordinal"),
    ("quarter", "string", "dimension", "nominal"),
    ("month_name", "string", "dimension", "nominal"),
    ("day_name", "string", "dimension", "nominal"),
    ("is_weekend", "integer", "dimension", "ordinal"),
    ("customer_id", "string", "dimension", "nominal"),
    ("country", "string", "dimension", "nominal"),
    ("state", "string", "dimension", "nominal"),
    ("region", "string", "dimension", "nominal"),
    ("product_id", "string", "dimension", "nominal"),
    ("category_name", "string", "dimension", "nominal"),
    ("brand_name", "string", "dimension", "nominal"),
    ("channel", "string", "dimension", "nominal"),
    ("coupon_flag", "integer", "measure", "quantitative"),
    ("quantity", "integer", "measure", "quantitative"),
    ("gross_amount", "real", "measure", "quantitative"),
    ("discount_amount", "integer", "measure", "quantitative"),
    ("net_amount", "real", "measure", "quantitative"),
    ("net_amount_inr", "integer", "measure", "quantitative"),
]
REMOTE = {"integer": ("20", "sint64"), "real": ("5", "double"), "string": ("129", "str"), "date": ("133", "date")}

CALCS = {
    # id: (caption, datatype, formula)
    "Calculation_1001": ("Revenue (USD)", "real", f"[net_amount_inr] / {FX_INR_PER_USD}"),
    "Calculation_1002": ("Orders", "integer", "COUNTD([transaction_id])"),
    "Calculation_1003": ("Avg Order Value (USD)", "real", "SUM([Calculation_1001]) / [Calculation_1002]"),
}

REV = f"[{DS}].[sum:Calculation_1001:qk]"
ORDERS = f"[{DS}].[usr:Calculation_1002:qk]"
AOV = f"[{DS}].[usr:Calculation_1003:qk]"


def calc_xml(cid: str) -> str:
    caption, dtype, formula = CALCS[cid]
    return (f"<column caption={q(caption)} datatype='{dtype}' name='[{cid}]' role='measure' type='quantitative'>"
            f"<calculation class='tableau' formula={q(formula)} /></column>")


def q(s: str) -> str:
    return "'" + escape(s, {"'": "&apos;"}) + "'"


def plain_col(name: str) -> str:
    _, dtype, role, typ = next(c for c in COLUMNS if c[0] == name)
    return f"<column datatype='{dtype}' name='[{name}]' role='{role}' type='{typ}' />"


def datasource_xml() -> str:
    rel_cols = "".join(
        f"<column datatype='{d}' name='{n}' ordinal='{i}' />" for i, (n, d, _, _) in enumerate(COLUMNS))
    records = "".join(
        "<metadata-record class='column'>"
        f"<remote-name>{n}</remote-name><remote-type>{REMOTE[d][0]}</remote-type>"
        f"<local-name>[{n}]</local-name><parent-name>[sales_report.csv]</parent-name>"
        f"<remote-alias>{n}</remote-alias><ordinal>{i}</ordinal><local-type>{d}</local-type>"
        f"<aggregation>{'Sum' if d in ('integer', 'real') else 'Count'}</aggregation><contains-null>true</contains-null>"
        f"<attributes><attribute datatype='string' name='DebugRemoteType'>&quot;{REMOTE[d][1]}&quot;</attribute></attributes>"
        "</metadata-record>"
        for i, (n, d, _, _) in enumerate(COLUMNS))
    cols = "".join(plain_col(n) for n, *_ in COLUMNS) + "".join(calc_xml(c) for c in CALCS)
    return (
        f"<datasource caption='sales_report' inline='true' name='{DS}' version='18.1'>"
        "<connection class='federated'><named-connections>"
        f"<named-connection caption='sales_report' name='{CONN}'>"
        f"<connection class='textscan' directory='{DATA_DIR}' filename='sales_report.csv' password='' server='' />"
        "</named-connection></named-connections>"
        f"<relation connection='{CONN}' name='sales_report.csv' table='[sales_report#csv]' type='table'>"
        f"<columns character-set='UTF-8' header='yes' locale='en_US' separator=','>{rel_cols}</columns></relation>"
        f"<metadata-records>{records}</metadata-records></connection>"
        "<aliases enabled='yes' />"
        f"{cols}{extract_xml()}</datasource>"
    )


def extract_xml() -> str:
    """Tableau Public only accepts extracts, so the workbook points at a packaged .hyper file."""
    records = "".join(
        "<metadata-record class='column'>"
        f"<remote-name>{n}</remote-name><remote-type>{REMOTE[d][0]}</remote-type>"
        f"<local-name>[{n}]</local-name><parent-name>[Extract]</parent-name>"
        f"<remote-alias>{n}</remote-alias><ordinal>{i}</ordinal><local-type>{d}</local-type>"
        f"<aggregation>{'Sum' if d in ('integer', 'real') else 'Count'}</aggregation><contains-null>true</contains-null>"
        + ("<collation flag='0' name='LROOT' />" if d == "string" else "")
        + "</metadata-record>"
        for i, (n, d, _, _) in enumerate(COLUMNS))
    return (
        "<extract count='-1' enabled='true' units='records'>"
        f"<connection access_mode='readonly' author-locale='en_US' class='hyper' dbname='{HYPER_PATH}' "
        "default-settings='hyper' schema='Extract' sslmode='' tablename='Extract' "
        "update-time='10/26/2026 1:42:00 PM' username='tableau_internal_user'>"
        "<relation name='Extract' table='[Extract].[Extract]' type='table' />"
        f"<metadata-records>{records}</metadata-records>"
        "</connection></extract>"
    )


def worksheet(name: str, title: str, deps: str, rows: str, cols: str, mark: str, text_enc: str = "") -> str:
    enc = f"<encodings><text column='{text_enc}' /></encodings>" if text_enc else ""
    return (
        f"<worksheet name={q(name)}>"
        f"<layout-options><title><formatted-text><run bold='true'>{escape(title)}</run></formatted-text></title></layout-options>"
        "<table><view>"
        f"<datasources><datasource caption='sales_report' name='{DS}' /></datasources>"
        f"<datasource-dependencies datasource='{DS}'>{deps}</datasource-dependencies>"
        "<aggregation value='true' /></view><style />"
        "<panes><pane selection-relaxation-option='selection-relaxation-allow'>"
        f"<view><breakdown value='auto' /></view><mark class='{mark}' />{enc}</pane></panes>"
        f"<rows>{rows}</rows><cols>{cols}</cols></table></worksheet>"
    )


def ci(column: str, derivation: str, name: str, pivot_type: str) -> str:
    return f"<column-instance column='[{column}]' derivation='{derivation}' name='[{name}]' pivot='key' type='{pivot_type}' />"


REV_DEPS = calc_xml("Calculation_1001") + ci("Calculation_1001", "Sum", "sum:Calculation_1001:qk", "quantitative")
ORD_DEPS = calc_xml("Calculation_1002") + ci("Calculation_1002", "User", "usr:Calculation_1002:qk", "quantitative")
AOV_DEPS = (calc_xml("Calculation_1001") + calc_xml("Calculation_1002") + calc_xml("Calculation_1003")
            + ci("Calculation_1003", "User", "usr:Calculation_1003:qk", "quantitative"))


def dim_deps(col: str) -> str:
    return plain_col(col) + ci(col, "None", f"none:{col}:nk", "nominal")


SHEETS = [
    worksheet("KPI Revenue", "Total Revenue (USD)", REV_DEPS, "", "", "Text", REV),
    worksheet("KPI Orders", "Orders", ORD_DEPS, "", "", "Text", ORDERS),
    worksheet("KPI AOV", "Avg Order Value (USD)", AOV_DEPS, "", "", "Text", AOV),
    worksheet("Revenue Trend", "Daily Revenue (USD)",
              plain_col("transaction_date") + ci("transaction_date", "Day-Trunc", "tdy:transaction_date:qk", "quantitative")
              + REV_DEPS, REV, f"[{DS}].[tdy:transaction_date:qk]", "Line"),
    worksheet("By Category", "Revenue by Category", dim_deps("category_name") + REV_DEPS,
              f"[{DS}].[none:category_name:nk]", REV, "Bar", REV),
    worksheet("By Country", "Revenue by Country", dim_deps("country") + REV_DEPS,
              f"[{DS}].[none:country:nk]", REV, "Bar", REV),
    worksheet("Mobile vs Website", "Revenue by Channel", dim_deps("channel") + REV_DEPS,
              REV, f"[{DS}].[none:channel:nk]", "Bar", REV),
]

# Dashboard layout: coordinates are in 1/100000 of the dashboard size.
ZONES = [
    ("KPI Revenue", 0, 7000, 33333, 15000),
    ("KPI Orders", 33333, 7000, 33333, 15000),
    ("KPI AOV", 66666, 7000, 33334, 15000),
    ("Revenue Trend", 0, 22000, 100000, 36000),
    ("By Category", 0, 58000, 40000, 42000),
    ("By Country", 40000, 58000, 35000, 42000),
    ("Mobile vs Website", 75000, 58000, 25000, 42000),
]
DASH = "E-Commerce Sales Dashboard"


def dashboard_xml() -> str:
    zones = "".join(
        f"<zone h='{h}' id='{i + 3}' name={q(n)} w='{w}' x='{x}' y='{y}' />" for i, (n, x, y, w, h) in enumerate(ZONES))
    title = ("<zone h='7000' id='2' type-v2='text' w='100000' x='0' y='0'><formatted-text>"
             "<run bold='true' fontsize='18'>E-Commerce Sales Dashboard</run>"
             "<run fontsize='11'>   |   Aug–Oct 2025 · 7 countries · Databricks gold layer</run>"
             "</formatted-text></zone>")
    return (f"<dashboard name={q(DASH)}><style />"
            "<size maxheight='900' maxwidth='1400' minheight='900' minwidth='1400' />"
            f"<zones><zone h='100000' id='1' type-v2='layout-basic' w='100000' x='0' y='0'>{title}{zones}</zone></zones>"
            "</dashboard>")


def workbook_xml() -> str:
    cards = ("<cards>"
             "<edge name='left'><strip size='160'><card type='pages' /><card type='filters' /><card type='marks' />"
             "</strip></edge>"
             "<edge name='top'><strip size='2147483647'><card type='columns' /></strip>"
             "<strip size='2147483647'><card type='rows' /></strip>"
             "<strip size='31'><card type='title' /></strip></edge>"
             "</cards>")
    windows = "".join(f"<window class='worksheet' name={q(n)}>{cards}</window>" for n, *_ in ZONES)
    viewpoints = "".join(f"<viewpoint name={q(n)}><zoom type='entire-view' /></viewpoint>" for n, *_ in ZONES)
    windows += (f"<window class='dashboard' maximized='true' name={q(DASH)}>"
                f"<viewpoints>{viewpoints}</viewpoints><active id='-1' /></window>")
    return (
        "<?xml version='1.0' encoding='utf-8' ?>\n"
        "<workbook original-version='18.1' source-build='2023.1.0 (20231.23.0310.1045)' source-platform='mac' "
        "version='18.1' xmlns:user='http://www.tableausoftware.com/xml/user'>"
        "<preferences />"
        f"<datasources>{datasource_xml()}</datasources>"
        f"<worksheets>{''.join(SHEETS)}</worksheets>"
        f"<dashboards>{dashboard_xml()}</dashboards>"
        f"<windows source-height='30'>{windows}</windows>"
        "</workbook>\n"
    )


def main() -> None:
    csv = Path(sys.argv[1] if len(sys.argv) > 1 else Path(__file__).with_name("sales_report.csv"))
    out_dir = Path(__file__).parent
    twb = workbook_xml()
    (out_dir / "ecommerce_sales.twb").write_text(twb, encoding="utf-8")
    with zipfile.ZipFile(out_dir / "ecommerce_sales.twbx", "w", zipfile.ZIP_DEFLATED) as z:
        z.writestr("ecommerce_sales.twb", twb)
        z.write(csv, f"{DATA_DIR}/sales_report.csv")
        z.write(out_dir / "sales_report.hyper", HYPER_PATH)  # built by make_extract.py
    print("wrote", out_dir / "ecommerce_sales.twbx")


if __name__ == "__main__":
    main()

import os
import sys
import requests
import pandas as pd

# Dossier de sortie Kafka
DIRECTORY_PATH = '/opt/airflow/dags/files/'

def json_rpc(endpoint, method, params):
    payload = {
        "jsonrpc": "2.0",
        "method": method,
        "params": params,
        "id": 1
    }
    response = requests.post(endpoint, json=payload).json()
    return response.get("result")

def fetch_data(start_date, end_date):
    try:
        url = 'https://bazardist.com'
        db = 'test.bazardist.com'
        username = 'a'
        password = '123'
        jsonrpc_url = f"{url}/jsonrpc"

        uid = json_rpc(jsonrpc_url, "call", {
            "service": "common",
            "method": "login",
            "args": [db, username, password]
        })

        invoice_ids = json_rpc(jsonrpc_url, "call", {
            "service": "object",
            "method": "execute_kw",
            "args": [db, uid, password, "account.move", "search", [[
                ("invoice_date", ">=", start_date),
                ("invoice_date", "<=", end_date),
                ("move_type", "in", ["out_invoice", "out_refund"])
            ]]]
        })

        invoices = json_rpc(jsonrpc_url, "call", {
            "service": "object",
            "method": "execute_kw",
            "args": [db, uid, password, "account.move", "read", [invoice_ids], {
                "fields": ["id", "invoice_date", "name", "partner_id", "payment_method", "invoice_line_ids"]
            }]
        })

        line_ids = sum([inv["invoice_line_ids"] for inv in invoices], [])
        lines = json_rpc(jsonrpc_url, "call", {
            "service": "object",
            "method": "execute_kw",
            "args": [db, uid, password, "account.move.line", "read", [line_ids], {
                "fields": ["id", "move_id", "product_id", "quantity", "price_unit", "price_subtotal"]
            }]
        })

        product_ids = list({l["product_id"][0] for l in lines if l["product_id"]})
        partner_ids = list({inv["partner_id"][0] for inv in invoices})

        products = json_rpc(jsonrpc_url, "call", {
            "service": "object",
            "method": "execute_kw",
            "args": [db, uid, password, "product.product", "read", [product_ids], {
                "fields": ["id", "default_code", "product_tmpl_id"]
            }]
        })

        tmpl_ids = list({p["product_tmpl_id"][0] for p in products if p["product_tmpl_id"]})
        templates = json_rpc(jsonrpc_url, "call", {
            "service": "object",
            "method": "execute_kw",
            "args": [db, uid, password, "product.template", "read", [tmpl_ids], {
                "fields": ["id", "name", "categ_id"]
            }]
        })

        categ_ids = list({t["categ_id"][0] for t in templates if t["categ_id"]})
        categories = json_rpc(jsonrpc_url, "call", {
            "service": "object",
            "method": "execute_kw",
            "args": [db, uid, password, "product.category", "read", [categ_ids], {
                "fields": ["id", "name"]
            }]
        })

        partners = json_rpc(jsonrpc_url, "call", {
            "service": "object",
            "method": "execute_kw",
            "args": [db, uid, password, "res.partner", "read", [partner_ids], {
                "fields": ["id", "name", "ref"]
            }]
        })

        df_lines = pd.DataFrame(lines)
        df_invoices = pd.DataFrame(invoices)
        df_products = pd.DataFrame(products)
        df_templates = pd.DataFrame(templates)
        df_categories = pd.DataFrame(categories)
        df_partners = pd.DataFrame(partners)

        df_lines["invoice_id"] = df_lines["move_id"].apply(lambda x: x[0])
        df_lines["product_id_clean"] = df_lines["product_id"].apply(lambda x: x[0] if x else None)

        df = df_lines.merge(df_invoices, left_on="invoice_id", right_on="id", suffixes=("", "_inv"))
        df = df.merge(df_products[["id", "default_code", "product_tmpl_id"]],
                      left_on="product_id_clean", right_on="id", suffixes=("", "_prod"))

        df["tmpl_id"] = df["product_tmpl_id"].apply(lambda x: x[0] if x else None)
        df = df.merge(df_templates[["id", "name", "categ_id"]],
                      left_on="tmpl_id", right_on="id", suffixes=("", "_tmpl"))

        df["categ_id"] = df["categ_id"].apply(lambda x: x[0] if x else None)
        df = df.merge(df_categories[["id", "name"]],
                      left_on="categ_id", right_on="id", suffixes=("", "_cat"))

        df["partner_id_clean"] = df["partner_id"].apply(lambda x: x[0] if x else None)
        df = df.merge(df_partners[["id", "name", "ref"]],
                      left_on="partner_id_clean", right_on="id", suffixes=("", "_partner"))

        df_result = df[[
            "ref", "name_partner", "invoice_date", "name",
            "default_code", "name_tmpl", "quantity", "price_unit",
            "price_subtotal", "name_cat", "payment_method"
        ]]
        df_result.rename(columns={
            "name_partner": "client",
            "name": "facture",
            "name_tmpl": "produit",
            "name_cat": "categorie"
        }, inplace=True)

        df_result["payment_method"] = df_result["payment_method"].apply(lambda x: 1 if x == "Espèces" else 2)

        os.makedirs(DIRECTORY_PATH, exist_ok=True)
        file_path = os.path.join(DIRECTORY_PATH, "ventes_export.csv")
        df_result.to_csv(file_path, index=False, sep=";", encoding="utf-8-sig")

        print(f" Données exportées avec succès vers : {file_path}")

    except Exception as e:
        print(f" Une erreur s’est produite : {str(e)}")

# Entrée CLI
if __name__ == "__main__":
    if len(sys.argv) != 3:
        print(" Usage : python odoo_export_to_files.py <date_début> <date_fin>")
        print(" Exemple : python odoo_export_to_files.py 2025-05-01 2025-05-10")
    else:
        fetch_data(sys.argv[1], sys.argv[2])

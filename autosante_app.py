"""
AutoSanté v2.1 — Odoo API direct · Paramètres via Google Sheets
Société : DI-Africa (Congo) SA uniquement (company_id = 3)

Installation :
    pip install streamlit openpyxl python-docx

Lancement :
    streamlit run autosante_app.py
"""

import re
import io
import csv
import zipfile
import calendar
import urllib.request
import urllib.parse
from datetime import datetime
from collections import defaultdict

import streamlit as st
import openpyxl
from openpyxl.styles import Font, PatternFill, Alignment, Border, Side
from openpyxl.utils import get_column_letter
import xmlrpc.client
from docx import Document

# ── CONFIGURATION ─────────────────────────────────────────────────────────
ODOO_URL   = "https://www.di-africa.com"
ODOO_DB    = "odoo-ps-psbe-di-africa-co1-main-30213273"
COMPANY_ID = 3   # DI-Africa (Congo) SA

# Identifiants Odoo — lus depuis Streamlit Secrets en prod,
# ou depuis les variables ci-dessous en local (fichier .streamlit/secrets.toml)
def _cfg(key: str, default: str) -> str:
    try:
        return st.secrets[key]
    except Exception:
        return default

ODOO_EMAIL = _cfg("ODOO_EMAIL", "")
ODOO_KEY   = _cfg("ODOO_KEY",   "")

# Google Sheet partagé pour Taux Clients + Prestataires
# → Partager la feuille "Paramètres" en "Tout le monde peut consulter"
# → Copier l'ID depuis l'URL : docs.google.com/spreadsheets/d/{ID}/edit
PARAMS_SHEET_ID = "1nkvAb3rjrqXF95K4ZYC5ZfIXqMusSesCGR8kQTw9dsw"  # Paramètres TBR

# Regex extraction client depuis le département Odoo
# "BU Congo / Clients Congo / ERoCo / Mengo"  →  "ERoCo"
_CLIENT_RE = re.compile(
    r'Clients Congo\s*/\s*([\w\d\s\-\.&()]+?)(?:\s*/\s*[\w\d\s]+\s*$|$)',
    re.IGNORECASE
)

# ── CONNEXION ODOO ────────────────────────────────────────────────────────
@st.cache_resource(show_spinner="Connexion à Odoo…")
def odoo_connect():
    common = xmlrpc.client.ServerProxy(f"{ODOO_URL}/xmlrpc/2/common")
    uid    = common.authenticate(ODOO_DB, ODOO_EMAIL, ODOO_KEY, {})
    if not uid:
        st.error("❌ Échec de connexion à Odoo. Vérifiez les identifiants.")
        st.stop()
    models = xmlrpc.client.ServerProxy(f"{ODOO_URL}/xmlrpc/2/object")
    return uid, models


def odoo_read(models, uid, model, domain, fields, limit=10000, order=None):
    kwargs = {"fields": fields, "limit": limit}
    if order:
        kwargs["order"] = order
    return models.execute_kw(ODOO_DB, uid, ODOO_KEY, model,
                             "search_read", [domain], kwargs)


# ── EXTRACTION ODOO ───────────────────────────────────────────────────────
@st.cache_data(ttl=300, show_spinner="Récupération des factures…")
def fetch_invoice_lines(_uid, _models, date_from: str, date_to: str):
    """Lignes de facture santé DI-Africa Congo sur la période."""
    # Filtre identique au filtre Odoo manuel d'Aurice :
    # "Invoice lines contains Medical" → product_id.name ilike "Medical"
    # + Optique pour les soins optiques
    domain = [
        ["company_id",            "=",  COMPANY_ID],
        ["move_id.state",         "=",  "posted"],
        ["date",                  ">=", date_from],
        ["date",                  "<=", date_to],
        ["x_studio_employee_inv", "!=", False],
        "|",
        ["product_id.name", "ilike", "Medical"],
        ["product_id.name", "ilike", "Optique"],
    ]
    fields = [
        "move_id", "product_id", "balance",
        "partner_id", "date", "x_studio_employee_inv",
    ]
    return odoo_read(_models, _uid, "account.move.line", domain, fields)


@st.cache_data(ttl=3600, show_spinner="Chargement des employés (actifs + archivés)…")
def fetch_employees(_uid, _models):
    """Employés DI-Africa Congo — actifs ET archivés pour garantir
    que les factures d'ex-employés sont bien rattachées."""
    rows = odoo_read(_models, _uid, "hr.employee",
                     [["company_id", "=", COMPANY_ID],
                      ["active", "in", [True, False]]],   # inclut les archivés
                     ["id", "name", "department_id", "active"], limit=5000)
    result = {}
    for r in rows:
        dept  = r["department_id"][1] if r["department_id"] else ""
        m     = _CLIENT_RE.search(dept)
        client = m.group(1).strip() if m else ""
        # Supprimer éventuel sous-site résiduel
        client = re.sub(r'\s*/.*$', '', client).strip()
        result[r["id"]] = {
            "name":   r["name"],
            "dept":   dept,
            "client": client,
            "active": r.get("active", True),   # False = employé archivé
        }
    return result


# ── LECTURE PARAMÈTRES DEPUIS GOOGLE SHEETS ───────────────────────────────
@st.cache_data(ttl=600, show_spinner="Chargement des paramètres depuis Google Sheets…")
def load_params_from_gsheet(sheet_id: str) -> dict:
    """
    Lit les feuilles 'Taux Clients' et 'Prestataires' depuis le Google Sheet partagé
    via export CSV (pas d'authentification requise — partage "tout le monde peut consulter").

    Retourne :
      params["rates"]        → {client: {cc, consult_soc, consult_emp,
                                          pharma_soc, pharma_emp, optique_soc, optique_emp}}
      params["prestataires"] → {nom_upper: type}  (type = Consultation | Pharmacie | Optique)
    """
    base_url = (
        f"https://docs.google.com/spreadsheets/d/{sheet_id}"
        "/gviz/tq?tqx=out:csv&sheet="
    )

    def fetch_sheet_csv(sheet_name: str) -> list:
        url = base_url + urllib.parse.quote(sheet_name)
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "AutoSante/2.1"})
            with urllib.request.urlopen(req, timeout=15) as resp:
                raw = resp.read().decode("utf-8")
            return list(csv.reader(raw.splitlines()))
        except Exception as e:
            st.error(f"❌ Impossible de lire la feuille « {sheet_name} » : {e}")
            st.stop()

    def _f(v, default=0.0):
        """
        Convertit une valeur de taux en float décimal (0.0–1.0).
        Gère : 0.8 · "80%" · "80" · "0,8" · cellule vide
        """
        s = str(v).replace(",", ".").strip()
        has_pct = s.endswith("%")
        s = s.rstrip("%").strip()
        try:
            result = float(s) if s else default
            # Si la valeur est en pourcentage (ex: 80 ou 80%) → diviser par 100
            if has_pct or result > 1.0:
                result /= 100.0
            return result
        except (ValueError, TypeError):
            return default

    # Taux Clients
    rates = {}
    taux_rows = fetch_sheet_csv("Taux Clients")
    for row in taux_rows[1:]:   # skip header
        if len(row) < 2 or not row[1].strip():
            continue
        client = row[1].strip()
        rates[client] = {
            "cc":          row[0].strip() if row[0] else "",
            "consult_soc": _f(row[2] if len(row) > 2 else 0),
            "consult_emp": _f(row[3] if len(row) > 3 else 0),
            "pharma_soc":  _f(row[4] if len(row) > 4 else 0),
            "pharma_emp":  _f(row[5] if len(row) > 5 else 0),
            "optique_soc": _f(row[6]) if len(row) > 6 and row[6].strip() else None,
            "optique_emp": _f(row[7]) if len(row) > 7 and row[7].strip() else None,
        }

    # Prestataires
    prest = {}
    prest_rows = fetch_sheet_csv("Prestataires")
    for row in prest_rows[1:]:  # skip header
        if len(row) >= 2 and row[0].strip():
            prest[row[0].strip().upper()] = row[1].strip()

    return {"rates": rates, "prestataires": prest}


# ── TRAITEMENT PRINCIPAL ──────────────────────────────────────────────────
def process_data(lines, employees, params) -> list:
    """
    Retourne une liste de dicts enrichis :
      employee_name, client, dept, prestataire, prest_type,
      product, date, invoice_ref,
      montant_total, part_soc, part_emp,
      warning (str ou None)
    """
    rates  = params["rates"]
    prests = params["prestataires"]
    rows   = []

    for ln in lines:
        emp_id   = ln["x_studio_employee_inv"][0]
        emp_name = ln["x_studio_employee_inv"][1]
        emp_info = employees.get(emp_id, {"name": emp_name, "dept": "", "client": ""})
        client   = emp_info["client"]

        partner_name = ln["partner_id"][1] if ln["partner_id"] else ""
        product_name = ln["product_id"][1] if ln["product_id"] else ""
        balance      = abs(float(ln["balance"] or 0))
        inv_ref      = ln["move_id"][1] if ln["move_id"] else ""

        # Type prestataire
        prest_type = prests.get(partner_name.upper(), None)
        if prest_type is None:
            prod_lower = product_name.lower()
            if "optique" in prod_lower:
                prest_type = "Optique"
            else:
                prest_type = "Pharmacie"  # défaut

        # Taux client (matching insensible à la casse)
        rate = None
        for k in rates:
            if k.lower() == client.lower():
                rate = rates[k]
                break
        # Fallback : matching partiel
        if rate is None:
            for k in rates:
                if client.lower() in k.lower() or k.lower() in client.lower():
                    rate = rates[k]
                    break

        warning = None
        if rate is None:
            warning = f"Client '{client}' introuvable dans Paramètres → taux 50/50 appliqué"
            rate = {"consult_soc": 0.5, "consult_emp": 0.5,
                    "pharma_soc": 0.5,  "pharma_emp": 0.5,
                    "optique_soc": None, "optique_emp": None, "cc": ""}

        # Calcul parts
        if prest_type == "Optique":
            cap = rate["optique_soc"]
            if cap:
                part_soc = min(balance, cap)
                part_emp = max(0.0, balance - cap)
            else:
                part_soc = balance * 0.5
                part_emp = balance * 0.5
        elif prest_type == "Consultation":
            part_soc = balance * rate["consult_soc"]
            part_emp = balance * rate["consult_emp"]
        else:  # Pharmacie
            part_soc = balance * rate["pharma_soc"]
            part_emp = balance * rate["pharma_emp"]

        rows.append({
            "employee_name": emp_name,
            "client":        client or "(non identifié)",
            "cc":            rate.get("cc", ""),
            "dept":          emp_info["dept"],
            "prestataire":   partner_name,
            "prest_type":    prest_type,
            "product":       product_name,
            "date":          ln["date"],
            "invoice_ref":   inv_ref,
            "montant_total": balance,
            "part_soc":      round(part_soc, 0),
            "part_emp":      round(part_emp, 0),
            "warning":       warning,
        })

    return rows


# ── GÉNÉRATION EXCEL GLOBAL ───────────────────────────────────────────────
def _style_header(ws, row_num, cols, bg="1F4E79", fg="FFFFFF"):
    fill   = PatternFill("solid", fgColor=bg)
    font   = Font(bold=True, color=fg, name="Calibri", size=10)
    border = Border(
        left=Side(style="thin"), right=Side(style="thin"),
        top=Side(style="thin"), bottom=Side(style="thin")
    )
    for col in range(1, cols + 1):
        c = ws.cell(row=row_num, column=col)
        c.fill   = fill
        c.font   = font
        c.border = border
        c.alignment = Alignment(horizontal="center", vertical="center", wrap_text=True)


def _style_data(ws, row_num, cols, alt=False):
    bg     = "F2F2F2" if alt else "FFFFFF"
    fill   = PatternFill("solid", fgColor=bg)
    border = Border(
        left=Side(style="thin"), right=Side(style="thin"),
        top=Side(style="thin"), bottom=Side(style="thin")
    )
    font = Font(name="Calibri", size=10)
    for col in range(1, cols + 1):
        c = ws.cell(row=row_num, column=col)
        c.fill   = fill
        c.border = border
        c.font   = font


def build_global_excel(rows: list, period_label: str) -> bytes:
    wb = openpyxl.Workbook()

    # ── Feuille Export (données brutes) ──────────────────────────────────
    ws = wb.active
    ws.title = "Export"
    headers = [
        "Date", "N° Facture", "Prestataire", "Type",
        "Employé(e)", "Client", "Produit",
        "Montant Total", "Part Société", "Part Employé(e)",
    ]
    ws.append(headers)
    _style_header(ws, 1, len(headers))

    for i, r in enumerate(rows):
        ws.append([
            r["date"], r["invoice_ref"], r["prestataire"], r["prest_type"],
            r["employee_name"], r["client"], r["product"],
            r["montant_total"], r["part_soc"], r["part_emp"],
        ])
        _style_data(ws, i + 2, len(headers), alt=(i % 2 == 1))
        # Format monétaire
        for col in [8, 9, 10]:
            ws.cell(row=i + 2, column=col).number_format = '#,##0" FCFA"'

    # Largeurs colonnes
    for col, w in zip(range(1, len(headers) + 1),
                      [12, 28, 32, 14, 32, 20, 22, 16, 16, 16]):
        ws.column_dimensions[get_column_letter(col)].width = w
    ws.freeze_panes = "A2"

    # ── Feuille TCD Clients ───────────────────────────────────────────────
    ws2 = wb.create_sheet("TCD Clients")
    hdrs = ["Client", "CC", "Consultation (Total)", "Pharmacie (Total)",
            "Optique (Total)", "Total", "Part Société", "Part Employé(e)"]
    ws2.append(hdrs)
    _style_header(ws2, 1, len(hdrs))

    from collections import defaultdict
    by_client = defaultdict(lambda: {"cc": "", "cons": 0, "phar": 0, "opti": 0,
                                      "soc": 0, "emp": 0})
    for r in rows:
        c = by_client[r["client"]]
        c["cc"] = r["cc"]
        if r["prest_type"] == "Consultation":
            c["cons"] += r["montant_total"]
        elif r["prest_type"] == "Optique":
            c["opti"] += r["montant_total"]
        else:
            c["phar"] += r["montant_total"]
        c["soc"] += r["part_soc"]
        c["emp"] += r["part_emp"]

    for i, (client, v) in enumerate(sorted(by_client.items())):
        total = v["cons"] + v["phar"] + v["opti"]
        ws2.append([client, v["cc"], v["cons"], v["phar"], v["opti"],
                    total, v["soc"], v["emp"]])
        _style_data(ws2, i + 2, len(hdrs), alt=(i % 2 == 1))
        for col in range(3, 9):
            ws2.cell(row=i + 2, column=col).number_format = '#,##0" FCFA"'

    for col, w in zip(range(1, len(hdrs) + 1),
                      [28, 12, 22, 18, 16, 16, 16, 16]):
        ws2.column_dimensions[get_column_letter(col)].width = w
    ws2.freeze_panes = "A2"

    # ── Feuille TCD Employés ──────────────────────────────────────────────
    ws3 = wb.create_sheet("TCD Employés")
    hdrs3 = ["Employé(e)", "Client", "Consultation", "Pharmacie",
             "Optique", "Total", "Part Société", "Part Employé(e)"]
    ws3.append(hdrs3)
    _style_header(ws3, 1, len(hdrs3))

    by_emp = defaultdict(lambda: {"client": "", "cons": 0, "phar": 0,
                                   "opti": 0, "soc": 0, "emp": 0})
    for r in rows:
        e = by_emp[r["employee_name"]]
        e["client"] = r["client"]
        if r["prest_type"] == "Consultation":
            e["cons"] += r["montant_total"]
        elif r["prest_type"] == "Optique":
            e["opti"] += r["montant_total"]
        else:
            e["phar"] += r["montant_total"]
        e["soc"] += r["part_soc"]
        e["emp"] += r["part_emp"]

    for i, (emp, v) in enumerate(sorted(by_emp.items())):
        total = v["cons"] + v["phar"] + v["opti"]
        ws3.append([emp, v["client"], v["cons"], v["phar"], v["opti"],
                    total, v["soc"], v["emp"]])
        _style_data(ws3, i + 2, len(hdrs3), alt=(i % 2 == 1))
        for col in range(3, 9):
            ws3.cell(row=i + 2, column=col).number_format = '#,##0" FCFA"'

    for col, w in zip(range(1, len(hdrs3) + 1),
                      [34, 28, 18, 16, 14, 16, 16, 16]):
        ws3.column_dimensions[get_column_letter(col)].width = w
    ws3.freeze_panes = "A2"

    # ── Titre et métadonnées ──────────────────────────────────────────────
    for sheet in [ws, ws2, ws3]:
        sheet.sheet_properties.tabColor = "1F4E79"

    buf = io.BytesIO()
    wb.save(buf)
    return buf.getvalue()


# ── GÉNÉRATION EXCEL INDIVIDUEL ───────────────────────────────────────────
def build_individual_excel(emp_name: str, emp_rows: list, period_label: str) -> bytes:
    wb  = openpyxl.Workbook()
    ws  = wb.active
    ws.title = emp_name[:31]  # max 31 chars pour onglet

    # En-tête
    ws.merge_cells("A1:J1")
    ws["A1"] = f"Relevé de consommation santé — {emp_name} — {period_label}"
    ws["A1"].font      = Font(bold=True, size=13, color="1F4E79", name="Calibri")
    ws["A1"].alignment = Alignment(horizontal="center")
    ws.row_dimensions[1].height = 24

    headers = ["Date", "N° Facture", "Prestataire", "Type",
               "Produit", "Montant Total", "Part Société", "Part Employé(e)"]
    ws.append(headers)
    _style_header(ws, 2, len(headers))

    total_soc = total_emp = 0
    for i, r in enumerate(emp_rows):
        ws.append([
            r["date"], r["invoice_ref"], r["prestataire"], r["prest_type"],
            r["product"], r["montant_total"], r["part_soc"], r["part_emp"],
        ])
        _style_data(ws, i + 3, len(headers), alt=(i % 2 == 1))
        for col in [6, 7, 8]:
            ws.cell(row=i + 3, column=col).number_format = '#,##0" FCFA"'
        total_soc += r["part_soc"]
        total_emp += r["part_emp"]

    # Ligne totaux
    last_data = 3 + len(emp_rows)
    ws.append(["", "", "", "", "TOTAL",
               total_soc + total_emp, total_soc, total_emp])
    _style_header(ws, last_data, len(headers), bg="2E75B6")
    for col in [6, 7, 8]:
        ws.cell(row=last_data, column=col).number_format = '#,##0" FCFA"'

    for col, w in zip(range(1, len(headers) + 1),
                      [12, 28, 32, 14, 22, 18, 16, 16]):
        ws.column_dimensions[get_column_letter(col)].width = w
    ws.freeze_panes = "A3"

    buf = io.BytesIO()
    wb.save(buf)
    return buf.getvalue()


# ── GÉNÉRATION RAPPORT CLIENT (REFACTURATION) ─────────────────────────────
def _taux_label(row: dict) -> str:
    """Affiche le taux/plafond en texte lisible pour le rapport client."""
    if row["prest_type"] == "Optique":
        # Optique : plafond fixe côté société
        return f"Plafond {row['part_soc']:,.0f} FCFA" if row["part_emp"] > 0 else "100 % soc"
    # Pourcentage côté société
    total = row["montant_total"]
    if total > 0:
        pct = round(row["part_soc"] / total * 100)
        return f"{pct} %"
    return "—"


def build_client_excel(client_name: str, client_rows: list, period_label: str) -> bytes:
    """
    Rapport de consommation par client — ce que DI-Africa refacture au client.
    Format identique aux anciens exports GAS :
      Date | N° Facture | Employé | Prestataire | Type | Montant Total
      | Taux/Plafond | Part Employé(e) | Part Employeur (Société)
    """
    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = client_name[:31]

    # ── En-tête titre ──────────────────────────────────────────────────────
    NCOLS = 9
    ws.merge_cells(f"A1:{get_column_letter(NCOLS)}1")
    ws["A1"] = f"Rapport de consommation — {client_name} — {period_label}"
    ws["A1"].font      = Font(bold=True, size=13, color="FFFFFF", name="Calibri")
    ws["A1"].fill      = PatternFill("solid", fgColor="1F4E79")
    ws["A1"].alignment = Alignment(horizontal="center", vertical="center")
    ws.row_dimensions[1].height = 28

    ws.merge_cells(f"A2:{get_column_letter(NCOLS)}2")
    ws["A2"] = "DI-Africa (Congo) SA · Direction Médicale · sante@di-africa.com"
    ws["A2"].font      = Font(italic=True, size=9, color="595959", name="Calibri")
    ws["A2"].alignment = Alignment(horizontal="center")
    ws.row_dimensions[2].height = 16

    # ── En-têtes colonnes ──────────────────────────────────────────────────
    headers = [
        "Date", "N° Facture", "Employé(e)", "Prestataire",
        "Type", "Montant Total", "Taux / Plafond",
        "Part Employé(e)", "Part Employeur",
    ]
    ws.append(headers)
    _style_header(ws, 3, NCOLS)

    # ── Données ────────────────────────────────────────────────────────────
    total_montant = total_emp = total_soc = 0
    for i, r in enumerate(client_rows):
        ws.append([
            r["date"],
            r["invoice_ref"],
            r["employee_name"],
            r["prestataire"],
            r["prest_type"],
            r["montant_total"],
            _taux_label(r),
            r["part_emp"],
            r["part_soc"],
        ])
        row_num = i + 4
        _style_data(ws, row_num, NCOLS, alt=(i % 2 == 1))
        for col in [6, 8, 9]:
            ws.cell(row=row_num, column=col).number_format = '#,##0" FCFA"'
        total_montant += r["montant_total"]
        total_emp     += r["part_emp"]
        total_soc     += r["part_soc"]

    # ── Ligne TOTAL ────────────────────────────────────────────────────────
    last = 4 + len(client_rows)
    ws.append(["", "", "", "", "TOTAL",
               total_montant, "", total_emp, total_soc])
    _style_header(ws, last, NCOLS, bg="2E75B6")
    for col in [6, 8, 9]:
        ws.cell(row=last, column=col).number_format = '#,##0" FCFA"'

    # ── Largeurs ───────────────────────────────────────────────────────────
    for col, w in zip(range(1, NCOLS + 1),
                      [12, 26, 34, 32, 14, 18, 16, 18, 18]):
        ws.column_dimensions[get_column_letter(col)].width = w
    ws.freeze_panes = "A4"

    buf = io.BytesIO()
    wb.save(buf)
    return buf.getvalue()


def build_clients_recap_excel(rows: list, period_label: str) -> bytes:
    """
    Récapitulatif multi-onglets : un onglet par client + un onglet synthèse.
    Utile pour le reporting global de refacturation.
    """
    wb = openpyxl.Workbook()

    # ── Onglet Synthèse ────────────────────────────────────────────────────
    ws_synth = wb.active
    ws_synth.title = "Synthèse"
    hdrs = ["Client", "CC", "Nb employés", "Consultation",
            "Pharmacie", "Optique", "Total", "Part Employé(e)", "Part Employeur"]
    ws_synth.append(hdrs)
    _style_header(ws_synth, 1, len(hdrs))

    by_client = defaultdict(lambda: {
        "cc": "", "emps": set(),
        "cons": 0, "phar": 0, "opti": 0, "emp": 0, "soc": 0
    })
    for r in rows:
        c = by_client[r["client"]]
        c["cc"] = r["cc"]
        c["emps"].add(r["employee_name"])
        if r["prest_type"] == "Consultation":
            c["cons"] += r["montant_total"]
        elif r["prest_type"] == "Optique":
            c["opti"] += r["montant_total"]
        else:
            c["phar"] += r["montant_total"]
        c["emp"] += r["part_emp"]
        c["soc"] += r["part_soc"]

    for i, (client, v) in enumerate(sorted(by_client.items())):
        total = v["cons"] + v["phar"] + v["opti"]
        ws_synth.append([client, v["cc"], len(v["emps"]),
                         v["cons"], v["phar"], v["opti"],
                         total, v["emp"], v["soc"]])
        _style_data(ws_synth, i + 2, len(hdrs), alt=(i % 2 == 1))
        for col in [4, 5, 6, 7, 8, 9]:
            ws_synth.cell(row=i + 2, column=col).number_format = '#,##0" FCFA"'

    for col, w in zip(range(1, len(hdrs) + 1),
                      [32, 10, 14, 18, 16, 14, 16, 18, 18]):
        ws_synth.column_dimensions[get_column_letter(col)].width = w
    ws_synth.freeze_panes = "A2"

    # ── Un onglet par client ───────────────────────────────────────────────
    by_client_rows = defaultdict(list)
    for r in rows:
        by_client_rows[r["client"]].append(r)

    for client, c_rows in sorted(by_client_rows.items()):
        ws = wb.create_sheet(title=client[:31])
        headers = ["Date", "N° Facture", "Employé(e)", "Prestataire",
                   "Type", "Montant Total", "Taux / Plafond",
                   "Part Employé(e)", "Part Employeur"]
        ws.append(headers)
        _style_header(ws, 1, len(headers))
        tot_m = tot_e = tot_s = 0
        for i, r in enumerate(c_rows):
            ws.append([r["date"], r["invoice_ref"], r["employee_name"],
                       r["prestataire"], r["prest_type"], r["montant_total"],
                       _taux_label(r), r["part_emp"], r["part_soc"]])
            _style_data(ws, i + 2, len(headers), alt=(i % 2 == 1))
            for col in [6, 8, 9]:
                ws.cell(row=i + 2, column=col).number_format = '#,##0" FCFA"'
            tot_m += r["montant_total"]
            tot_e += r["part_emp"]
            tot_s += r["part_soc"]
        last = 2 + len(c_rows)
        ws.append(["", "", "", "", "TOTAL", tot_m, "", tot_e, tot_s])
        _style_header(ws, last, len(headers), bg="2E75B6")
        for col in [6, 8, 9]:
            ws.cell(row=last, column=col).number_format = '#,##0" FCFA"'
        for col, w in zip(range(1, len(headers) + 1),
                          [12, 26, 34, 32, 14, 18, 16, 18, 18]):
            ws.column_dimensions[get_column_letter(col)].width = w
        ws.freeze_panes = "A2"

    buf = io.BytesIO()
    wb.save(buf)
    return buf.getvalue()


# ── GÉNÉRATION RELEVÉ AUTOMATIQUE PAR EMPLOYÉ (DOCX) ─────────────────────
def build_releve_employe_docx(emp_name: str, client: str,
                               emp_rows: list, period_label: str) -> bytes:
    """
    Relevé de consommation individuel généré automatiquement (sans template).
    Format identique aux anciens exports : en-tête DI-Africa, tableau,
    colonne Taux/Plafond, ligne TOTAL.
    """
    from docx.shared import Pt, Cm, RGBColor
    from docx.enum.text import WD_ALIGN_PARAGRAPH
    from docx.enum.table import WD_ALIGN_VERTICAL
    from docx.oxml.ns import qn
    from docx.oxml import OxmlElement

    doc = Document()

    # ── Marges ──────────────────────────────────────────────────────────────
    for section in doc.sections:
        section.top_margin    = Cm(1.8)
        section.bottom_margin = Cm(1.8)
        section.left_margin   = Cm(2.0)
        section.right_margin  = Cm(2.0)

    def _para(text="", bold=False, size=11, color=None, align=WD_ALIGN_PARAGRAPH.LEFT):
        p = doc.add_paragraph()
        p.alignment = align
        run = p.add_run(text)
        run.bold = bold
        run.font.size = Pt(size)
        run.font.name = "Calibri"
        if color:
            run.font.color.rgb = RGBColor(*color)
        p.paragraph_format.space_before = Pt(0)
        p.paragraph_format.space_after  = Pt(2)
        return p

    def _set_cell_bg(cell, hex_color):
        tc = cell._tc
        tcPr = tc.get_or_add_tcPr()
        shd = OxmlElement("w:shd")
        shd.set(qn("w:val"),   "clear")
        shd.set(qn("w:color"), "auto")
        shd.set(qn("w:fill"),  hex_color)
        tcPr.append(shd)

    def _set_cell_borders(table):
        for row in table.rows:
            for cell in row.cells:
                tc = cell._tc
                tcPr = tc.get_or_add_tcPr()
                tcBorders = OxmlElement("w:tcBorders")
                for side in ["top", "left", "bottom", "right"]:
                    border = OxmlElement(f"w:{side}")
                    border.set(qn("w:val"),   "single")
                    border.set(qn("w:sz"),    "4")
                    border.set(qn("w:space"), "0")
                    border.set(qn("w:color"), "BFBFBF")
                    tcBorders.append(border)
                tcPr.append(tcBorders)

    # ── En-tête société ──────────────────────────────────────────────────────
    _para("DI Africa (Congo) SA", bold=True, size=13,
          color=(31, 78, 121), align=WD_ALIGN_PARAGRAPH.CENTER)
    _para("Pointe-Noire, Congo  ·  sante@di-africa.com",
          size=9, color=(89, 89, 89), align=WD_ALIGN_PARAGRAPH.CENTER)
    doc.add_paragraph()

    _para("RELEVÉ DE CONSOMMATION SANTÉ", bold=True, size=14,
          color=(31, 78, 121), align=WD_ALIGN_PARAGRAPH.CENTER)
    _para(f"{emp_name}  ·  {client}  ·  {period_label}",
          size=11, align=WD_ALIGN_PARAGRAPH.CENTER)
    doc.add_paragraph()

    # ── Tableau ──────────────────────────────────────────────────────────────
    col_headers = ["Date", "N° Facture", "Prestataire", "Type",
                   "Montant Total", "Taux / Plafond", "Part Employé(e)", "Part Employeur"]
    col_widths  = [Cm(2.2), Cm(3.4), Cm(4.2), Cm(2.8),
                   Cm(2.8), Cm(2.8), Cm(2.8), Cm(2.8)]

    table = doc.add_table(rows=1, cols=len(col_headers))
    table.style = None  # pas de style nommé pour éviter les conflits

    # Largeurs colonnes
    for i, w in enumerate(col_widths):
        for cell in table.columns[i].cells:
            cell.width = w

    # Ligne en-tête
    hdr_cells = table.rows[0].cells
    for i, h in enumerate(col_headers):
        hdr_cells[i].text = h
        hdr_cells[i].paragraphs[0].alignment = WD_ALIGN_PARAGRAPH.CENTER
        run = hdr_cells[i].paragraphs[0].runs[0]
        run.bold = True
        run.font.size = Pt(9)
        run.font.name = "Calibri"
        run.font.color.rgb = RGBColor(255, 255, 255)
        _set_cell_bg(hdr_cells[i], "1F4E79")

    # Lignes données
    total_montant = total_emp = total_soc = 0
    for idx, r in enumerate(emp_rows):
        row_cells = table.add_row().cells
        bg = "F2F2F2" if idx % 2 == 1 else "FFFFFF"
        values = [
            str(r["date"]),
            r["invoice_ref"],
            r["prestataire"],
            r["prest_type"],
            f"{r['montant_total']:,.0f}",
            _taux_label(r),
            f"{r['part_emp']:,.0f}",
            f"{r['part_soc']:,.0f}",
        ]
        for i, v in enumerate(values):
            row_cells[i].text = v
            p = row_cells[i].paragraphs[0]
            p.alignment = WD_ALIGN_PARAGRAPH.RIGHT if i >= 4 else WD_ALIGN_PARAGRAPH.LEFT
            run = p.runs[0] if p.runs else p.add_run(v)
            run.font.size = Pt(9)
            run.font.name = "Calibri"
            _set_cell_bg(row_cells[i], bg)
        total_montant += r["montant_total"]
        total_emp     += r["part_emp"]
        total_soc     += r["part_soc"]

    # Ligne TOTAL
    tot_cells = table.add_row().cells
    tot_vals  = ["", "", "", "TOTAL",
                 f"{total_montant:,.0f}", "",
                 f"{total_emp:,.0f}", f"{total_soc:,.0f}"]
    for i, v in enumerate(tot_vals):
        tot_cells[i].text = v
        p = tot_cells[i].paragraphs[0]
        p.alignment = WD_ALIGN_PARAGRAPH.RIGHT if i >= 4 else WD_ALIGN_PARAGRAPH.LEFT
        run = p.runs[0] if p.runs else p.add_run(v)
        run.bold = True
        run.font.size = Pt(9)
        run.font.name = "Calibri"
        run.font.color.rgb = RGBColor(255, 255, 255)
        _set_cell_bg(tot_cells[i], "2E75B6")

    _set_cell_borders(table)

    # ── Pied de page ─────────────────────────────────────────────────────────
    doc.add_paragraph()
    _para(
        "S.A au capital de 20 000 000 FCFA · Rue Germain Bicoumat, Pointe-Noire, CONGO\n"
        "N° RCCM CG-PNR-01-2021-B14-00003 · NIU M210000001927504 · 04 063 6397",
        size=7, color=(127, 127, 127), align=WD_ALIGN_PARAGRAPH.CENTER
    )

    buf = io.BytesIO()
    doc.save(buf)
    return buf.getvalue()


# ── GÉNÉRATION BON DE CONSOMMATION (DOCX) ─────────────────────────────────
def fill_bon_template(template_bytes: bytes, emp_name: str,
                      emp_rows: list, period_label: str,
                      dest_type: str) -> bytes:
    """
    Remplace <<NOM>> et <<TABLEAU_CONSOMMATIONS>> dans le template .docx.
    dest_type = 'Employeur' ou 'Employé'
    """
    doc = Document(io.BytesIO(template_bytes))

    # Remplacement <<NOM>>
    for para in doc.paragraphs:
        if "<<NOM>>" in para.text:
            for run in para.runs:
                run.text = run.text.replace("<<NOM>>", emp_name)
        if "<<PERIODE>>" in para.text:
            for run in para.runs:
                run.text = run.text.replace("<<PERIODE>>", period_label)

    # Remplacement <<TABLEAU_CONSOMMATIONS>>
    for para in doc.paragraphs:
        if "<<TABLEAU_CONSOMMATIONS>>" in para.text:
            # Vider le paragraphe placeholder
            para.clear()
            # Corriger les marges si elles sont stockées en float dans le template
            # (bug courant avec certains templates Word → ValueError in python-docx)
            # On patch directement les attributs XML pour convertir float → int
            import re as _re
            for section in doc.sections:
                pgMar = section._sectPr.find(
                    ".//{http://schemas.openxmlformats.org/wordprocessingml/2006/main}pgMar"
                )
                if pgMar is not None:
                    ns = "http://schemas.openxmlformats.org/wordprocessingml/2006/main"
                    for attr in ["left", "right", "top", "bottom", "header", "footer", "gutter"]:
                        key = f"{{{ns}}}{attr}"
                        val = pgMar.get(key)
                        if val and "." in val:
                            pgMar.set(key, str(int(float(val))))
            # Construire un tableau dans le document
            table = doc.add_table(rows=1, cols=5)
            # Ne pas forcer un style nommé — le template peut ne pas l'inclure
            # On appliquera des bordures manuellement si nécessaire
            hdr_cells = table.rows[0].cells
            for i, h in enumerate(["Date", "Prestataire", "Montant", "Part Société", "Part Employé(e)"]):
                hdr_cells[i].text = h
                hdr_cells[i].paragraphs[0].runs[0].bold = True

            total_soc = total_emp = 0
            for r in emp_rows:
                row_cells = table.add_row().cells
                row_cells[0].text = str(r["date"])
                row_cells[1].text = r["prestataire"]
                row_cells[2].text = f"{r['montant_total']:,.0f} FCFA"
                row_cells[3].text = f"{r['part_soc']:,.0f} FCFA"
                row_cells[4].text = f"{r['part_emp']:,.0f} FCFA"
                total_soc += r["part_soc"]
                total_emp += r["part_emp"]

            # Ligne totaux
            tot_cells = table.add_row().cells
            tot_cells[0].text = "TOTAL"
            tot_cells[0].paragraphs[0].runs[0].bold = True
            tot_cells[2].text = f"{total_soc + total_emp:,.0f} FCFA"
            tot_cells[3].text = f"{total_soc:,.0f} FCFA"
            tot_cells[4].text = f"{total_emp:,.0f} FCFA"

            # Déplacer le tableau avant le paragraphe placeholder
            para._element.addprevious(table._element)
            break

    buf = io.BytesIO()
    doc.save(buf)
    return buf.getvalue()


# ── INTERFACE STREAMLIT ───────────────────────────────────────────────────
def main():
    st.set_page_config(
        page_title="AutoSanté — DI-Africa Congo",
        page_icon="🏥",
        layout="wide",
    )

    st.title("🏥 AutoSanté — DI-Africa (Congo) SA")
    st.caption("Génération des rapports de consommation santé · Version Odoo API direct")

    # ── Connexion Odoo ────────────────────────────────────────────────────
    uid, models = odoo_connect()
    st.success(f"✅ Connecté à Odoo (`{ODOO_URL}`) · Société : DI-Africa (Congo) SA")

    st.divider()

    # ── Paramètres depuis Google Sheets (automatique) ─────────────────────
    col_gs1, col_gs2 = st.columns([3, 1])
    with col_gs1:
        st.info(
            "📊 Les taux clients et prestataires sont chargés automatiquement "
            "depuis le **Google Sheet Paramètres** partagé. "
            "Aucun fichier à uploader."
        )
    with col_gs2:
        sheet_url = (f"https://docs.google.com/spreadsheets/d/{PARAMS_SHEET_ID}/edit")
        st.markdown(f"[🔗 Ouvrir le Google Sheet]({sheet_url})", unsafe_allow_html=False)
        if st.button("🔄 Recharger les paramètres"):
            load_params_from_gsheet.clear()
            st.rerun()

    # ── Sélection période ─────────────────────────────────────────────────
    col3, col4 = st.columns(2)
    with col3:
        month = st.selectbox("Mois", range(1, 13),
                             format_func=lambda m: datetime(2000, m, 1).strftime("%B").capitalize(),
                             index=datetime.today().month - 2 if datetime.today().month > 1 else 0)
    with col4:
        year  = st.number_input("Année", min_value=2020,
                                max_value=2030, value=datetime.today().year)

    last_day   = calendar.monthrange(year, month)[1]
    date_from  = f"{year}-{month:02d}-01"
    date_to    = f"{year}-{month:02d}-{last_day:02d}"
    period_label = f"{datetime(year, month, 1).strftime('%B %Y').capitalize()}"
    st.caption(f"Période : **{date_from}** → **{date_to}**")

    st.divider()

    # ── Templates Bons de consommation (optionnel) ────────────────────────
    with st.expander("📄 Templates Bon de consommation (optionnel)"):
        tpl_emp    = st.file_uploader("Template Employé",   type=["docx"], key="tpl_emp")
        tpl_empeur = st.file_uploader("Template Employeur", type=["docx"], key="tpl_empeur")

    st.divider()

    # ── Bouton de génération ──────────────────────────────────────────────
    if st.button("🚀 Générer les rapports", type="primary", use_container_width=True):

        with st.spinner("Récupération des données Odoo…"):
            lines     = fetch_invoice_lines(uid, models, date_from, date_to)
            employees = fetch_employees(uid, models)

        st.info(f"📊 {len(lines)} lignes de facturation récupérées pour {period_label}")

        if not lines:
            st.warning("Aucune ligne trouvée pour cette période. Vérifiez les dates.")
            st.stop()

        with st.spinner("Chargement des paramètres…"):
            params = load_params_from_gsheet(PARAMS_SHEET_ID)
        st.success(f"✅ Paramètres chargés : "
                   f"{len(params['rates'])} clients · "
                   f"{len(params['prestataires'])} prestataires")

        with st.spinner("Calcul des parts société / employé(e)…"):
            rows = process_data(lines, employees, params)

        # Avertissements
        warnings = [r for r in rows if r["warning"]]
        if warnings:
            with st.expander(f"⚠️ {len(warnings)} avertissement(s) de matching"):
                for w in set(r["warning"] for r in warnings):
                    st.warning(w)

        # ── Génération fichiers ───────────────────────────────────────────
        with st.spinner("Génération des fichiers…"):
            zip_buf = io.BytesIO()
            with zipfile.ZipFile(zip_buf, "w", zipfile.ZIP_DEFLATED) as zf:

                # Export global
                global_xls = build_global_excel(rows, period_label)
                zf.writestr(f"Export_Global_{period_label.replace(' ', '_')}.xlsx",
                            global_xls)

                # ── Rapports clients (refacturation) ──────────────────────
                # 1. Récapitulatif multi-onglets (synthèse + un onglet/client)
                recap_xls = build_clients_recap_excel(rows, period_label)
                zf.writestr(
                    f"Clients/Recap_Clients_{period_label.replace(' ', '_')}.xlsx",
                    recap_xls
                )
                # 2. Un fichier Excel par client
                by_client_rows = defaultdict(list)
                for r in rows:
                    by_client_rows[r["client"]].append(r)
                for client_name, c_rows in by_client_rows.items():
                    safe_client = re.sub(r'[\\/*?:"<>|]', "_", client_name)
                    client_xls = build_client_excel(client_name, c_rows, period_label)
                    zf.writestr(
                        f"Clients/{safe_client}.xlsx",
                        client_xls
                    )

                # Fichiers individuels + bons de consommation
                by_emp = defaultdict(list)
                for r in rows:
                    by_emp[r["employee_name"]].append(r)

                for emp_name, emp_rows in by_emp.items():
                    safe_name = re.sub(r'[\\/*?:"<>|]', "_", emp_name)
                    emp_client = emp_rows[0]["client"] if emp_rows else ""

                    # Excel individuel
                    ind_xls = build_individual_excel(emp_name, emp_rows, period_label)
                    zf.writestr(f"Individuels/{safe_name}.xlsx", ind_xls)

                    # Relevé automatique .docx (sans template)
                    releve = build_releve_employe_docx(
                        emp_name, emp_client, emp_rows, period_label
                    )
                    zf.writestr(f"Relevés/{safe_name}.docx", releve)

                    # Bons de consommation (si templates fournis)
                    if tpl_emp:
                        tpl_emp.seek(0)
                        bon_emp = fill_bon_template(
                            tpl_emp.read(), emp_name, emp_rows,
                            period_label, "Employé"
                        )
                        zf.writestr(f"Bons_Employe/{safe_name}_bon_employe.docx", bon_emp)

                    if tpl_empeur:
                        tpl_empeur.seek(0)
                        bon_empeur = fill_bon_template(
                            tpl_empeur.read(), emp_name, emp_rows,
                            period_label, "Employeur"
                        )
                        zf.writestr(f"Bons_Employeur/{safe_name}_bon_employeur.docx",
                                    bon_empeur)

        zip_buf.seek(0)

        # ── Résumé ────────────────────────────────────────────────────────
        total_global = sum(r["montant_total"] for r in rows)
        total_soc    = sum(r["part_soc"]      for r in rows)
        total_emp    = sum(r["part_emp"]      for r in rows)

        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Employés concernés",  len(by_emp))
        c2.metric("Total consommations", f"{total_global:,.0f} FCFA")
        c3.metric("Part Société",        f"{total_soc:,.0f} FCFA")
        c4.metric("Part Employé(e)",     f"{total_emp:,.0f} FCFA")

        st.success(f"✅ {len(by_emp)} fichiers individuels générés")

        # ── Téléchargement ────────────────────────────────────────────────
        st.download_button(
            label="📥 Télécharger tous les rapports (.zip)",
            data=zip_buf,
            file_name=f"AutoSante_{period_label.replace(' ', '_')}.zip",
            mime="application/zip",
            use_container_width=True,
            type="primary",
        )

    st.divider()

    # ── Phase 2 : Retenues sur salaire ────────────────────────────────────
    with st.expander("🔒 Phase 2 — Retenues sur salaire (à venir)", expanded=False):
        st.info(
            "**Cette fonctionnalité sera disponible dans une prochaine version.**\n\n"
            "Elle permettra de :\n"
            "- Générer automatiquement le plan de retenue mensuel sur salaire "
            "(Part Employé(e) → déduction paie)\n"
            "- Gérer le **report cumulatif** : si le salaire ne couvre pas l'intégralité "
            "du mois, le solde est reporté au mois suivant\n"
            "- Produire un fichier `Retenues_MOIS.xlsx` avec l'historique par employé(e)\n"
            "- Synchronisation optionnelle avec Odoo Payroll\n\n"
            "*Demande initiale : Aurice Bouamba — mars 2026*"
        )
        st.button("🔔 Me notifier quand disponible", disabled=True,
                  help="Fonctionnalité non encore implémentée")


# ── PHASE 2 : RETENUES SUR SALAIRE (À VENIR) ─────────────────────────────
def _phase2_placeholder():
    """
    Section désactivée — intégration future des retenues sur salaire.

    Contexte (email Aurice, mars 2026) :
      - Chaque mois, la Part Employé(e) calculée doit être déduite du salaire
      - Le suivi doit être CUMULATIF : reporter le solde restant si le salaire
        ne couvre pas l'intégralité de la retenue du mois
      - Historique mensuel par employé(e) : mois, montant déduit, solde restant
      - À synchroniser avec le logiciel de paie (Odoo Payroll ou export manuel)

    Implémentation prévue :
      1. Stocker un récapitulatif mensuel dans Google Sheets (feuille "Retenues")
      2. À chaque génération, calculer le delta restant (mois précédent + nouveau)
      3. Générer un fichier "Retenues_MOIS.xlsx" avec le plan de déduction par employé
      4. Export optionnel vers Odoo Payroll via API (write sur hr.payslip.line)
    """
    pass   # sera activé en Phase 2


if __name__ == "__main__":
    main()

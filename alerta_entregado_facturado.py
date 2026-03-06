"""
================================================
 ODOO: Alerta Entregado vs Facturado
 ─────────────────────────────────────────────
 - Pedidos desde 01/03/2025 en adelante
 - Excluye diferencias cubiertas por NC
 - Ventana de gracia: 3 horas desde última entrega
 - Alerta solo la primera vez (sin spam)
 - Recordatorio si pasan 2 días sin resolver
 - Email con resumen + Excel adjunto
================================================
"""

import xmlrpc.client
import pandas as pd
import smtplib
import io
import json
import os
import subprocess
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
from datetime import datetime, timezone, timedelta

# ──────────────────────────────────────────────
# CONFIGURACIÓN
# ──────────────────────────────────────────────
ODOO_URL      = "https://temponovo.odoo.com"
ODOO_DB       = "cmcorpcl-temponovo-main-24490235"
ODOO_USER     = "natalia@temponovo.cl"
ODOO_PASSWORD = "Contraodoo94+"

EMAIL_TO      = os.environ.get("EMAIL_TO") or "natalia@temponovo.cl"

SMTP_HOST     = os.environ.get("SMTP_HOST") or "srv10.akkuarios.com"
SMTP_PORT     = int(os.environ.get("SMTP_PORT") or "587")
SMTP_USER     = os.environ.get("SMTP_USER") or "reportes@temponovo.cl"
SMTP_PASS     = os.environ.get("SMTP_PASS", "")

FECHA_INICIO      = "2025-03-01"  # ignorar todo lo anterior
HORAS_GRACIA      = 3             # horas desde entrega antes de alertar
DIAS_RECORDATORIO = 2             # días sin resolver para mandar recordatorio
MEMORIA_FILE      = "alertas_memoria.json"

# ── Para Colab:          SOLO_PREVIEW = True
# ── Para GitHub Actions: SOLO_PREVIEW = False
SOLO_PREVIEW = False

# ──────────────────────────────────────────────
# MEMORIA
# ──────────────────────────────────────────────
def cargar_memoria():
    if os.path.exists(MEMORIA_FILE):
        with open(MEMORIA_FILE, "r") as f:
            return json.load(f)
    return {}

def guardar_memoria(memoria):
    with open(MEMORIA_FILE, "w") as f:
        json.dump(memoria, f, indent=2, default=str)

def guardar_memoria_git():
    """Commit y push del JSON de memoria en GitHub Actions."""
    try:
        subprocess.run(["git", "config", "user.email", "actions@github.com"], check=True)
        subprocess.run(["git", "config", "user.name",  "GitHub Actions"],     check=True)
        subprocess.run(["git", "add", MEMORIA_FILE],                          check=True)
        result = subprocess.run(["git", "diff", "--cached", "--quiet"], capture_output=True)
        if result.returncode != 0:
            subprocess.run(["git", "commit", "-m",
                f"chore: actualizar memoria alertas {datetime.now().strftime('%Y-%m-%d %H:%M')}"],
                check=True)
            subprocess.run(["git", "push"], check=True)
            print("✅ Memoria guardada en GitHub")
        else:
            print("   Memoria sin cambios, no se hace commit")
    except Exception as e:
        print(f"⚠️  No se pudo hacer git push: {e}")

# ──────────────────────────────────────────────
# CONEXIÓN ODOO
# ──────────────────────────────────────────────
def conectar():
    common = xmlrpc.client.ServerProxy(f"{ODOO_URL}/xmlrpc/2/common")
    uid    = common.authenticate(ODOO_DB, ODOO_USER, ODOO_PASSWORD, {})
    if not uid:
        raise Exception("❌ No se pudo autenticar en Odoo.")
    models = xmlrpc.client.ServerProxy(f"{ODOO_URL}/xmlrpc/2/object")
    return uid, models

# ──────────────────────────────────────────────
# DIFERENCIAS REALES
# ──────────────────────────────────────────────
def get_diferencias(uid, models):
    fecha_corte = f"{FECHA_INICIO} 00:00:00"
    print(f"📅 Revisando pedidos desde {FECHA_INICIO}...")

    lineas = models.execute_kw(
        ODOO_DB, uid, ODOO_PASSWORD,
        "sale.order.line", "search_read",
        [[
            ["order_id.state",      "in", ["sale", "done"]],
            ["order_id.date_order", ">=", fecha_corte],
        ]],
        {"fields": ["id", "order_id", "product_id", "product_uom_qty",
                    "qty_delivered", "invoice_lines", "price_subtotal"]}
    )
    print(f"   {len(lineas)} líneas encontradas")
    if not lineas:
        return pd.DataFrame()

    # Cliente y fecha
    order_ids_all = list({l["order_id"][0] for l in lineas})
    pedidos_info  = models.execute_kw(
        ODOO_DB, uid, ODOO_PASSWORD,
        "sale.order", "search_read",
        [[["id", "in", order_ids_all]]],
        {"fields": ["id", "partner_id", "date_order"]}
    )
    pedido_map = {p["id"]: p for p in pedidos_info}

    # Facturado y NC
    all_inv_ids = []
    for l in lineas:
        all_inv_ids.extend(l.get("invoice_lines", []))

    qty_facturada = {}
    qty_nc        = {}

    if all_inv_ids:
        print("🧾 Trayendo facturas y NC...")
        inv_lines = models.execute_kw(
            ODOO_DB, uid, ODOO_PASSWORD,
            "account.move.line", "search_read",
            [[
                ["id", "in", all_inv_ids],
                ["move_id.state", "=", "posted"],
                ["move_id.move_type", "in", ["out_invoice", "out_refund"]],
            ]],
            {"fields": ["id", "quantity", "move_id"]}
        )
        move_ids = list({il["move_id"][0] for il in inv_lines})
        moves    = models.execute_kw(
            ODOO_DB, uid, ODOO_PASSWORD,
            "account.move", "search_read",
            [[["id", "in", move_ids]]],
            {"fields": ["id", "move_type"]}
        )
        move_type_map = {m["id"]: m["move_type"] for m in moves}

        inv_line_to_sol = {}
        for l in lineas:
            for inv_id in l.get("invoice_lines", []):
                inv_line_to_sol[inv_id] = l["id"]

        for il in inv_lines:
            sol_id    = inv_line_to_sol.get(il["id"])
            move_type = move_type_map.get(il["move_id"][0], "")
            if not sol_id:
                continue
            if move_type == "out_invoice":
                qty_facturada[sol_id] = qty_facturada.get(sol_id, 0) + il["quantity"]
            elif move_type == "out_refund":
                qty_nc[sol_id]        = qty_nc.get(sol_id, 0)        + il["quantity"]

    rows = []
    for l in lineas:
        sid       = l["id"]
        facturado = qty_facturada.get(sid, 0.0)
        nc        = qty_nc.get(sid, 0.0)
        neto      = facturado - nc
        entregado = l["qty_delivered"]
        diff      = entregado - neto
        pedido    = pedido_map.get(l["order_id"][0], {})

        if abs(diff) > 0.01:
            rows.append({
                "order_id":   l["order_id"][0],
                "pedido":     l["order_id"][1],
                "cliente":    pedido.get("partner_id", ["", ""])[1],
                "fecha":      pedido.get("date_order", "")[:10],
                "producto":   l["product_id"][1] if l["product_id"] else "",
                "pedido_qty": l["product_uom_qty"],
                "entregado":  entregado,
                "facturado":  facturado,
                "nc":         nc,
                "neto":       neto,
                "diferencia": diff,
                "subtotal":   l["price_subtotal"],
            })

    return pd.DataFrame(rows)

# ──────────────────────────────────────────────
# FILTRO VENTANA DE GRACIA
# ──────────────────────────────────────────────
def filtrar_gracia(df, uid, models):
    if df.empty:
        return df

    order_ids = df["order_id"].unique().tolist()
    print("🚚 Consultando fechas de entrega...")

    pickings = models.execute_kw(
        ODOO_DB, uid, ODOO_PASSWORD,
        "stock.picking", "search_read",
        [[
            ["sale_id", "in", order_ids],
            ["state",   "=", "done"],
            ["picking_type_code", "=", "outgoing"],
        ]],
        {"fields": ["sale_id", "date_done"]}
    )
    ultima = {}
    for p in pickings:
        oid   = p["sale_id"][0]
        fecha = datetime.fromisoformat(p["date_done"]).replace(tzinfo=timezone.utc)
        if oid not in ultima or fecha > ultima[oid]:
            ultima[oid] = fecha

    ahora = datetime.now(timezone.utc)
    df = df.copy()
    df["horas_desde_entrega"] = df["order_id"].apply(
        lambda oid: round((ahora - ultima[oid]).total_seconds() / 3600, 1)
        if oid in ultima else None
    )
    return df[
        df["horas_desde_entrega"].apply(lambda h: h is not None and h >= HORAS_GRACIA)
    ].reset_index(drop=True)

# ──────────────────────────────────────────────
# LÓGICA DE MEMORIA
# ──────────────────────────────────────────────
def filtrar_por_memoria(df, memoria):
    ahora     = datetime.now(timezone.utc)
    threshold = timedelta(days=DIAS_RECORDATORIO)
    nuevos, recordatorio = [], []

    for pedido in df["pedido"].unique():
        if pedido not in memoria:
            nuevos.append(pedido)
        else:
            ultimo = datetime.fromisoformat(memoria[pedido]["ultimo_envio"]).replace(tzinfo=timezone.utc)
            if (ahora - ultimo) >= threshold:
                recordatorio.append(pedido)
            # else: ya alertado, sin recordatorio aún → silencio

    return nuevos, recordatorio

def actualizar_memoria(memoria, alertas_activas, nuevos, recordatorio):
    ahora = datetime.now(timezone.utc).isoformat()

    for p in nuevos:
        memoria[p] = {"primera_alerta": ahora, "ultimo_envio": ahora}
    for p in recordatorio:
        memoria[p]["ultimo_envio"] = ahora

    # Limpiar resueltos
    activos = set(alertas_activas["pedido"].unique())
    for p in list(memoria.keys()):
        if p not in activos:
            del memoria[p]
            print(f"   ✅ {p} resuelto → removido de memoria")

    return memoria

# ──────────────────────────────────────────────
# EMAIL HTML
# ──────────────────────────────────────────────
def generar_html(resumen, nuevos, recordatorio_list):
    ahora_str = datetime.now().strftime("%d/%m/%Y %H:%M")
    n         = len(resumen)
    solo_rec  = len(nuevos) == 0
    tag       = "[RECORDATORIO] " if solo_rec else ""

    partes = []
    if nuevos:          partes.append(f"{len(nuevos)} nuevo(s)")
    if recordatorio_list: partes.append(f"{len(recordatorio_list)} recordatorio(s)")

    filas = ""
    for _, row in resumen.iterrows():
        color    = "#991b1b" if row["diferencia"] > 0 else "#854d0e"
        bg       = "#fee2e2" if row["diferencia"] > 0 else "#fef9c3"
        es_rec   = row["pedido"] in recordatorio_list
        rec_tag  = (' <span style="background:#fef3c7;color:#92400e;padding:1px 6px;'
                    'border-radius:20px;font-size:10px;">🔁 recordatorio</span>') if es_rec else ""
        badge    = (f'<span style="background:{bg};color:{color};padding:2px 8px;'
                    f'border-radius:20px;font-size:10px;font-weight:600;">{row["estado"]}</span>')

        filas += f"""
        <tr style="border-bottom:1px solid #f1f5f9;">
          <td style="padding:10px 12px;font-weight:600;">{row['pedido']}{rec_tag}</td>
          <td style="padding:10px 12px;">{row['cliente']}</td>
          <td style="padding:10px 12px;text-align:center;font-family:monospace;font-size:12px;">{row['fecha']}</td>
          <td style="padding:10px 12px;text-align:center;">{row['qty_pedida']:g}</td>
          <td style="padding:10px 12px;text-align:center;">{row['qty_entregada']:g}</td>
          <td style="padding:10px 12px;text-align:center;">{row['qty_facturada']:g}</td>
          <td style="padding:10px 12px;text-align:center;font-weight:700;color:{color};">{row['diferencia']:+g}</td>
          <td style="padding:10px 12px;">{badge}</td>
        </tr>"""

    html = f"""
    <div style="font-family:Arial,sans-serif;background:#f1f5f9;padding:24px;">
    <div style="max-width:900px;margin:auto;background:#fff;border-radius:12px;
                box-shadow:0 2px 8px rgba(0,0,0,.1);overflow:hidden;">
      <div style="background:#1e3a5f;color:#fff;padding:24px 32px;">
        <h2 style="margin:0;font-size:20px;">⚠️ {tag}Diferencia Entregado vs Facturado</h2>
        <p style="margin:6px 0 0;opacity:.75;font-size:13px;">
          {ahora_str} · {n} pedido(s) · {" · ".join(partes)}
        </p>
      </div>
      <div style="padding:24px 32px;">
        <p style="color:#475569;font-size:14px;margin-top:0;">
          Los siguientes pedidos tienen diferencia entre lo <strong>entregado</strong> y
          lo <strong>facturado neto</strong> (descontando NC) con más de
          <strong>{HORAS_GRACIA} horas</strong> sin resolverse.
          El detalle por producto va adjunto en Excel.
        </p>
        <table style="width:100%;border-collapse:collapse;font-size:13px;">
          <thead>
            <tr style="background:#f8fafc;">
              {''.join(f'<th style="padding:10px 12px;text-align:{"left" if i<2 else "center"};font-size:10px;text-transform:uppercase;letter-spacing:.05em;color:#94a3b8;border-bottom:1px solid #e2e8f0;">{h}</th>'
                for i, h in enumerate(["N° Pedido","Cliente","Fecha","Qty Pedida","Qty Entregada","Qty Facturada","Diferencia","Estado"]))}
            </tr>
          </thead>
          <tbody>{filas}</tbody>
        </table>
        <p style="font-size:11px;color:#94a3b8;margin-top:20px;margin-bottom:0;
                  border-top:1px solid #f1f5f9;padding-top:14px;">
          Sistema de alertas automáticas · Temponovo · Odoo · Cada 3h, lunes a viernes
        </p>
      </div>
    </div>
    </div>"""

    return tag, html

# ──────────────────────────────────────────────
# EXCEL
# ──────────────────────────────────────────────
def generar_excel(resumen, alertas):
    cols = ["pedido", "cliente", "fecha", "producto", "pedido_qty",
            "entregado", "facturado", "nc", "neto", "diferencia", "horas_desde_entrega"]
    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as writer:
        resumen.to_excel(writer, sheet_name="Resumen por pedido",   index=False)
        alertas[cols].sort_values(["pedido","producto"]).to_excel(
            writer, sheet_name="Detalle por producto", index=False)
    buf.seek(0)
    return buf

# ──────────────────────────────────────────────
# ENVÍO
# ──────────────────────────────────────────────
def enviar(html, excel_buf, nombre_xl, tag, n):
    dests = [e.strip() for e in EMAIL_TO.split(",")]
    msg   = MIMEMultipart("mixed")
    msg["Subject"] = f"⚠️ {tag}[{n} pedido(s)] Diferencia entregado vs facturado — Temponovo"
    msg["From"]    = SMTP_USER
    msg["To"]      = ", ".join(dests)
    msg.attach(MIMEText(html, "html"))

    adj = MIMEBase("application", "octet-stream")
    adj.set_payload(excel_buf.read())
    encoders.encode_base64(adj)
    adj.add_header("Content-Disposition", f'attachment; filename="{nombre_xl}"')
    msg.attach(adj)

    with smtplib.SMTP(SMTP_HOST, SMTP_PORT) as s:
        s.starttls()
        s.login(SMTP_USER, SMTP_PASS)
        s.sendmail(SMTP_USER, dests, msg.as_string())
    print(f"✅ Email enviado a: {EMAIL_TO}")

# ──────────────────────────────────────────────
# MAIN
# ──────────────────────────────────────────────
def main():
    print(f"\n🔍 Revisión — {datetime.now().strftime('%d/%m/%Y %H:%M')}")
    print("=" * 50)

    uid, models = conectar()
    print("✅ Conectado\n")

    # 1. Diferencias
    df = get_diferencias(uid, models)
    if df.empty:
        print("✅ Sin diferencias. Fin.")
        return

    # 2. Ventana de gracia
    alertas = filtrar_gracia(df, uid, models)
    print(f"   {len(alertas)} líneas superan {HORAS_GRACIA}h")
    if alertas.empty:
        print("✅ Todo dentro de la ventana de gracia. Fin.")
        return

    # 3. Memoria
    memoria = cargar_memoria()
    nuevos, recordatorio_list = filtrar_por_memoria(alertas, memoria)
    print(f"   Nuevos: {len(nuevos)} · Recordatorios: {len(recordatorio_list)}")

    if not nuevos and not recordatorio_list:
        print("✅ Sin alertas nuevas. Actualizando memoria...")
        memoria = actualizar_memoria(memoria, alertas, [], [])
        guardar_memoria(memoria)
        if not SOLO_PREVIEW:
            guardar_memoria_git()
        return

    # 4. Solo los pedidos a enviar
    a_enviar   = set(nuevos) | set(recordatorio_list)
    df_envio   = alertas[alertas["pedido"].isin(a_enviar)].copy()

    resumen = df_envio.groupby(["pedido","cliente","fecha"], as_index=False).agg(
        qty_pedida    = ("pedido_qty", "sum"),
        qty_entregada = ("entregado",  "sum"),
        qty_facturada = ("neto",       "sum"),
        diferencia    = ("diferencia", "sum"),
    )
    resumen["estado"] = resumen["diferencia"].apply(
        lambda d: "🔴 Entregado sin facturar" if d > 0 else "🟡 Facturado de más"
    )
    resumen = resumen.sort_values("diferencia", ascending=False).reset_index(drop=True)

    # 5. Generar contenido
    tag, html  = generar_html(resumen, nuevos, recordatorio_list)
    excel_buf  = generar_excel(resumen, df_envio)
    nombre_xl  = f"alerta_entregado_facturado_{datetime.now().strftime('%Y%m%d_%H%M')}.xlsx"

    # 6. Actualizar memoria
    memoria = actualizar_memoria(memoria, alertas, nuevos, recordatorio_list)
    guardar_memoria(memoria)

    # 7. Enviar o preview
    if SOLO_PREVIEW:
        try:
            from IPython.display import display, HTML as IHTML
            print("\n📧 PREVIEW DEL EMAIL:")
            print("─" * 50)
            display(IHTML(html))
        except ImportError:
            pass
        print(f"\n⚙️  SOLO_PREVIEW = True → no enviado")
        print(f"   Nuevos:         {nuevos}")
        print(f"   Recordatorios:  {recordatorio_list}")
        print(f"   Excel:          {nombre_xl} ({len(df_envio)} filas)")
    else:
        enviar(html, excel_buf, nombre_xl, tag, len(resumen))
        guardar_memoria_git()

    print("\n✅ Proceso completado.")

if __name__ == "__main__":
    main()

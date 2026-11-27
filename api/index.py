"""Crayon Roofings - Sales to Dispatch app (app.crayonroofings.com)
FastAPI + Neon Postgres, deployed on Vercel.
Flow: Enquiry -> Quotation -> Sales Order (approval) -> Cutting Program -> Dispatch
"""
import os, re, math, hmac, urllib.error, hashlib, time, json, secrets, datetime as dt, html, urllib.request, urllib.parse
from pathlib import Path
from decimal import Decimal, ROUND_HALF_UP

import psycopg
from psycopg.rows import dict_row
from fastapi import FastAPI, Request, HTTPException, Depends, Response
from fastapi.responses import HTMLResponse, JSONResponse

BUILD = "C10"
DB_URL = os.environ.get("DATABASE_URL", "")
SECRET = os.environ.get("SECRET_KEY", "change-me-in-vercel")
COOKIE = "crs_session"

app = FastAPI(title="Crayon Sales", docs_url=None, redoc_url=None)

# ---------------------------------------------------------------- database
SCHEMA = """
CREATE TABLE IF NOT EXISTS users (
  id SERIAL PRIMARY KEY, name TEXT NOT NULL, email TEXT UNIQUE NOT NULL, mobile TEXT,
  roles TEXT NOT NULL DEFAULT 'sales', pw_hash TEXT NOT NULL, active BOOLEAN DEFAULT TRUE,
  created_at TIMESTAMPTZ DEFAULT now());
CREATE TABLE IF NOT EXISTS password_resets (
  id SERIAL PRIMARY KEY, user_id INT REFERENCES users(id) ON DELETE CASCADE, code_hash TEXT NOT NULL,
  expires_at TIMESTAMPTZ NOT NULL, attempts INT DEFAULT 0, used BOOLEAN DEFAULT FALSE, created_at TIMESTAMPTZ DEFAULT now());
CREATE TABLE IF NOT EXISTS leads (
  id SERIAL PRIMARY KEY, lead_no TEXT UNIQUE, lead_date DATE DEFAULT CURRENT_DATE, company TEXT NOT NULL,
  contact_person TEXT, designation TEXT, mobile TEXT, alt_mobile TEXT, email TEXT, gstin TEXT,
  address TEXT, area TEXT, city TEXT, district TEXT, pincode TEXT,
  customer_type TEXT, source TEXT, referred_by TEXT, products TEXT, requirement TEXT, application TEXT,
  approx_qty TEXT, expected_value NUMERIC(14,2) DEFAULT 0, expected_close DATE, competitor TEXT,
  priority TEXT DEFAULT 'Warm', status TEXT DEFAULT 'New', owner_id INT REFERENCES users(id), next_follow_up DATE,
  lost_reason TEXT, remarks TEXT, customer_id INT REFERENCES customers(id), enquiry_id INT REFERENCES enquiries(id),
  created_by INT REFERENCES users(id), created_at TIMESTAMPTZ DEFAULT now(), updated_at TIMESTAMPTZ DEFAULT now());
CREATE TABLE IF NOT EXISTS visits (
  id SERIAL PRIMARY KEY, visit_no TEXT UNIQUE, visit_date DATE DEFAULT CURRENT_DATE, check_in TIMESTAMPTZ DEFAULT now(),
  lead_id INT REFERENCES leads(id), customer_id INT REFERENCES customers(id), party_name TEXT,
  visit_type TEXT, purpose TEXT, location TEXT, latitude NUMERIC(10,6), longitude NUMERIC(10,6),
  person_met TEXT, person_designation TEXT, person_mobile TEXT, discussion TEXT, outcome TEXT,
  order_potential NUMERIC(14,2) DEFAULT 0, next_action TEXT, next_follow_up DATE, photo TEXT,
  user_id INT REFERENCES users(id), created_at TIMESTAMPTZ DEFAULT now());
CREATE TABLE IF NOT EXISTS trips (
  id SERIAL PRIMARY KEY, trip_no TEXT UNIQUE, user_id INT REFERENCES users(id), trip_date DATE DEFAULT CURRENT_DATE,
  started_at TIMESTAMPTZ DEFAULT now(), start_lat NUMERIC(10,6), start_lng NUMERIC(10,6), start_place TEXT, start_odo NUMERIC(10,1),
  ended_at TIMESTAMPTZ, end_lat NUMERIC(10,6), end_lng NUMERIC(10,6), end_place TEXT, end_odo NUMERIC(10,1),
  gps_km NUMERIC(10,2) DEFAULT 0, vehicle TEXT, status TEXT DEFAULT 'Active', remarks TEXT);
CREATE TABLE IF NOT EXISTS trip_points (
  id SERIAL PRIMARY KEY, trip_id INT REFERENCES trips(id) ON DELETE CASCADE, ts TIMESTAMPTZ DEFAULT now(),
  lat NUMERIC(10,6) NOT NULL, lng NUMERIC(10,6) NOT NULL, accuracy NUMERIC(8,1), kind TEXT DEFAULT 'track',
  visit_id INT REFERENCES visits(id), label TEXT);
CREATE INDEX IF NOT EXISTS trip_points_trip ON trip_points(trip_id, ts);
CREATE TABLE IF NOT EXISTS counters (key TEXT PRIMARY KEY, val INT NOT NULL DEFAULT 0);
CREATE TABLE IF NOT EXISTS settings (key TEXT PRIMARY KEY, val TEXT);
CREATE TABLE IF NOT EXISTS customers (
  id SERIAL PRIMARY KEY, name TEXT NOT NULL, gstin TEXT, contact_person TEXT, mobile TEXT, email TEXT,
  address TEXT, city TEXT, state TEXT DEFAULT 'Tamil Nadu', credit_limit NUMERIC(14,2) DEFAULT 0,
  active BOOLEAN DEFAULT TRUE, created_at TIMESTAMPTZ DEFAULT now());
CREATE TABLE IF NOT EXISTS products (
  id SERIAL PRIMARY KEY, code TEXT UNIQUE, name TEXT NOT NULL, category TEXT DEFAULT 'Sheet',
  uom TEXT DEFAULT 'rft', rate NUMERIC(12,2) DEFAULT 0, gst_pct NUMERIC(5,2) DEFAULT 18,
  kg_per_rft NUMERIC(10,4) DEFAULT 0, cover_width_ft NUMERIC(8,3) DEFAULT 0, hsn TEXT,
  active BOOLEAN DEFAULT TRUE);
CREATE TABLE IF NOT EXISTS enquiries (
  id SERIAL PRIMARY KEY, enq_no TEXT UNIQUE, enq_date DATE DEFAULT CURRENT_DATE, source TEXT,
  customer_id INT REFERENCES customers(id), contact_name TEXT, mobile TEXT, site_location TEXT,
  requirement TEXT, owner_id INT REFERENCES users(id), status TEXT DEFAULT 'Open',
  follow_up DATE, lost_reason TEXT, created_at TIMESTAMPTZ DEFAULT now());
CREATE TABLE IF NOT EXISTS quotes (
  id SERIAL PRIMARY KEY, quote_no TEXT NOT NULL, rev INT DEFAULT 0, enquiry_id INT REFERENCES enquiries(id),
  customer_id INT REFERENCES customers(id) NOT NULL, quote_date DATE DEFAULT CURRENT_DATE,
  valid_till DATE, discount_pct NUMERIC(5,2) DEFAULT 0, freight NUMERIC(12,2) DEFAULT 0,
  subtotal NUMERIC(14,2) DEFAULT 0, discount_amt NUMERIC(14,2) DEFAULT 0, gst_amt NUMERIC(14,2) DEFAULT 0,
  total NUMERIC(14,2) DEFAULT 0, terms TEXT, notes TEXT, status TEXT DEFAULT 'Draft',
  created_by INT REFERENCES users(id), created_at TIMESTAMPTZ DEFAULT now(), UNIQUE(quote_no, rev));
CREATE TABLE IF NOT EXISTS quote_items (
  id SERIAL PRIMARY KEY, quote_id INT REFERENCES quotes(id) ON DELETE CASCADE, product_id INT REFERENCES products(id),
  description TEXT, colour TEXT, length_ft NUMERIC(10,3) DEFAULT 0, pcs NUMERIC(10,2) DEFAULT 0,
  qty NUMERIC(14,3) DEFAULT 0, uom TEXT, rate NUMERIC(12,2) DEFAULT 0, gst_pct NUMERIC(5,2) DEFAULT 18,
  amount NUMERIC(14,2) DEFAULT 0, sort INT DEFAULT 0);
CREATE TABLE IF NOT EXISTS orders (
  id SERIAL PRIMARY KEY, so_no TEXT UNIQUE, quote_id INT REFERENCES quotes(id), customer_id INT REFERENCES customers(id),
  so_date DATE DEFAULT CURRENT_DATE, delivery_date DATE, customer_po TEXT, total NUMERIC(14,2) DEFAULT 0,
  advance NUMERIC(14,2) DEFAULT 0, status TEXT DEFAULT 'Pending Approval', approval_reason TEXT,
  approved_by INT REFERENCES users(id), approved_at TIMESTAMPTZ, approval_note TEXT, site_address TEXT,
  notes TEXT, created_by INT REFERENCES users(id), created_at TIMESTAMPTZ DEFAULT now());
CREATE TABLE IF NOT EXISTS order_items (
  id SERIAL PRIMARY KEY, order_id INT REFERENCES orders(id) ON DELETE CASCADE, product_id INT REFERENCES products(id),
  description TEXT, colour TEXT, length_ft NUMERIC(10,3), pcs NUMERIC(10,2), qty NUMERIC(14,3), uom TEXT,
  rate NUMERIC(12,2), gst_pct NUMERIC(5,2), amount NUMERIC(14,2), sort INT DEFAULT 0);
CREATE TABLE IF NOT EXISTS cutting (
  id SERIAL PRIMARY KEY, order_id INT REFERENCES orders(id) ON DELETE CASCADE, order_item_id INT REFERENCES order_items(id),
  length_ft NUMERIC(10,3), pcs NUMERIC(10,2), coil_no TEXT, machine TEXT, plan_date DATE DEFAULT CURRENT_DATE,
  status TEXT DEFAULT 'Planned', done_pcs NUMERIC(10,2) DEFAULT 0, remarks TEXT, updated_at TIMESTAMPTZ DEFAULT now());
CREATE TABLE IF NOT EXISTS dispatches (
  id SERIAL PRIMARY KEY, dc_no TEXT UNIQUE, order_id INT REFERENCES orders(id), dc_date DATE DEFAULT CURRENT_DATE,
  vehicle_no TEXT, driver_name TEXT, driver_mobile TEXT, transporter TEXT, invoice_no TEXT, eway_bill_no TEXT,
  status TEXT DEFAULT 'Loading', delivered_on DATE, pod_note TEXT, remarks TEXT,
  created_by INT REFERENCES users(id), created_at TIMESTAMPTZ DEFAULT now());
CREATE TABLE IF NOT EXISTS dispatch_items (
  id SERIAL PRIMARY KEY, dispatch_id INT REFERENCES dispatches(id) ON DELETE CASCADE,
  order_item_id INT REFERENCES order_items(id), pcs NUMERIC(10,2), qty NUMERIC(14,3));
"""

DEFAULT_SETTINGS = {
    "max_discount_pct": "3",
    "quote_validity_days": "7",
    "company_name": "Crayon Roofings & Structures",
    "company_gstin": "",
    "company_address": "",
    "company_phone": "74012 33000",
    "company_email": "info@crayonroofings.com",
    "quote_terms": "1. Prices are ex-works unless freight is shown.\n2. GST as applicable.\n3. 100% payment before dispatch unless credit is approved.\n4. Lengths are cut to order; cut sheets are not returnable.\n5. Quote valid till the date shown.",
}

SAMPLE_PRODUCTS = [  # placeholders - replace with the real rate list
    ("JSW-CC-050", "JSW Colour Coated Sheet 0.50 mm (Trapezoidal)", "Sheet", "rft", 0, 18, 1.35, 3.5, "7210"),
    ("JSW-CC-045", "JSW Colour Coated Sheet 0.45 mm (Trapezoidal)", "Sheet", "rft", 0, 18, 1.22, 3.5, "7210"),
    ("JSW-GL-050", "JSW Galvalume Sheet 0.50 mm", "Sheet", "rft", 0, 18, 1.35, 3.5, "7210"),
    ("RIDGE-CC", "Ridge Cover (Colour Coated)", "Accessory", "rft", 0, 18, 0, 0, "7308"),
    ("FLASH-CC", "Flashing (Colour Coated)", "Accessory", "rft", 0, 18, 0, 0, "7308"),
    ("TATA-SDS-55", "Tata Agrico Self Drilling Screw 5.5x25", "Screw", "nos", 0, 18, 0, 0, "7318"),
    ("TATA-E6013-315", "Tata E6013 Welding Electrode 3.15 mm", "Consumable", "kg", 0, 18, 0, 0, "8311"),
]

_ready = False


def db():
    if not DB_URL:
        raise HTTPException(500, "DATABASE_URL is not set")
    c = psycopg.connect(DB_URL, row_factory=dict_row, autocommit=True)
    try:
        yield c
    finally:
        c.close()


def ensure_schema(c):
    global _ready
    if _ready:
        return
    c.execute(SCHEMA)
    for k, v in DEFAULT_SETTINGS.items():
        c.execute("INSERT INTO settings(key,val) VALUES(%s,%s) ON CONFLICT DO NOTHING", (k, v))
    if c.execute("SELECT count(*) n FROM products").fetchone()["n"] == 0:
        for p in SAMPLE_PRODUCTS:
            c.execute("INSERT INTO products(code,name,category,uom,rate,gst_pct,kg_per_rft,cover_width_ft,hsn)"
                      " VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s)", p)
    if c.execute("SELECT count(*) n FROM users").fetchone()["n"] == 0:
        em, pw = os.environ.get("ADMIN_EMAIL"), os.environ.get("ADMIN_PASSWORD")
        if em and pw:
            c.execute("INSERT INTO users(name,email,roles,pw_hash) VALUES('Admin',%s,'admin',%s)", (em.lower(), hash_pw(pw)))
    _ready = True


def fy_code(d=None):
    d = d or dt.date.today()
    y = d.year if d.month >= 4 else d.year - 1
    return f"{y % 100:02d}-{(y + 1) % 100:02d}"


def next_no(c, prefix):
    fy = fy_code()
    key = f"{prefix}/{fy}"
    n = c.execute("INSERT INTO counters(key,val) VALUES(%s,1) ON CONFLICT(key) DO UPDATE SET val=counters.val+1 RETURNING val",
                  (key,)).fetchone()["val"]
    return f"CRS/{prefix}/{fy}/{n:04d}"


def settings(c):
    return {r["key"]: r["val"] for r in c.execute("SELECT key,val FROM settings")}


# ---------------------------------------------------------------- auth
def hash_pw(pw, salt=None):
    salt = salt or os.urandom(16).hex()
    return salt + "$" + hashlib.pbkdf2_hmac("sha256", pw.encode(), salt.encode(), 200_000).hex()


def check_pw(pw, stored):
    try:
        salt = stored.split("$", 1)[0]
        return hmac.compare_digest(hash_pw(pw, salt), stored)
    except Exception:
        return False


def make_token(uid):
    payload = f"{uid}.{int(time.time()) + 7 * 86400}"
    return payload + "." + hmac.new(SECRET.encode(), payload.encode(), "sha256").hexdigest()


def read_token(tok):
    try:
        uid, exp, sig = tok.split(".")
        good = hmac.new(SECRET.encode(), f"{uid}.{exp}".encode(), "sha256").hexdigest()
        if hmac.compare_digest(good, sig) and int(exp) > time.time():
            return int(uid)
    except Exception:
        pass
    return None


def roles_of(u):
    return {r.strip() for r in (u.get("roles") or "").split(",") if r.strip()}


def user(request: Request, c=Depends(db)):
    ensure_schema(c)
    uid = read_token(request.cookies.get(COOKIE, ""))
    u = uid and c.execute("SELECT id,name,email,mobile,roles,active FROM users WHERE id=%s", (uid,)).fetchone()
    if not u or not u["active"]:
        raise HTTPException(401, "Please log in")
    u["role_set"] = roles_of(u)
    return u


def need(u, *roles):
    if "admin" in u["role_set"] or u["role_set"] & set(roles):
        return
    raise HTTPException(403, "You do not have access to this action")


def D(x):
    try:
        return Decimal(str(x if x not in (None, "") else 0))
    except Exception:
        return Decimal(0)


def r2(x):
    return x.quantize(Decimal("0.01"), ROUND_HALF_UP)


def clean(body, fields):
    return {k: (body.get(k) if body.get(k) != "" else None) for k in fields if k in body}


def upsert(c, table, fields, body, rid=None):
    data = clean(body, fields)
    if not data:
        raise HTTPException(400, "Nothing to save")
    if rid:
        sets = ", ".join(f"{k}=%s" for k in data)
        row = c.execute(f"UPDATE {table} SET {sets} WHERE id=%s RETURNING *", (*data.values(), rid)).fetchone()
        if not row:
            raise HTTPException(404, "Not found")
        return row
    cols = ", ".join(data)
    ph = ", ".join(["%s"] * len(data))
    return c.execute(f"INSERT INTO {table}({cols}) VALUES({ph}) RETURNING *", tuple(data.values())).fetchone()


# ---------------------------------------------------------------- auth routes
@app.post("/api/login")
async def login(request: Request, c=Depends(db)):
    ensure_schema(c)
    b = await request.json()
    u = c.execute("SELECT * FROM users WHERE email=%s AND active", ((b.get("email") or "").strip().lower(),)).fetchone()
    if not u or not check_pw(b.get("password") or "", u["pw_hash"]):
        raise HTTPException(401, "Wrong email or password")
    r = JSONResponse({"ok": True, "name": u["name"]})
    r.set_cookie(COOKIE, make_token(u["id"]), max_age=7 * 86400, httponly=True, samesite="lax",
                 secure=request.url.scheme == "https")
    return r



# ---------------------------------------------------------------- forgot password (6-digit code on WhatsApp via WATI)
WATI_URL = re.sub(r"/api/v1/?$", "", os.environ.get("WATI_API_URL", "").strip().rstrip("/"))
WATI_TOKEN = os.environ.get("WATI_TOKEN", "").strip()
WATI_OTP_TEMPLATE = os.environ.get("WATI_OTP_TEMPLATE", "").strip() or "otp_password2"
WA_READY = bool(WATI_URL and WATI_TOKEN)
NOT_SET = "Password reset on WhatsApp is not set up yet. Ask your admin to reset your password in Admin > Users."


def wa_number(mobile):
    d = re.sub(r"\D", "", mobile or "")
    if len(d) == 10:
        d = "91" + d
    elif len(d) == 11 and d.startswith("0"):
        d = "91" + d[1:]
    return d if len(d) == 12 and d.startswith("91") else ""


def send_wa_code(number, code):
    if not WA_READY:
        raise HTTPException(503, NOT_SET)
    tok = WATI_TOKEN if WATI_TOKEN.lower().startswith("bearer ") else "Bearer " + WATI_TOKEN
    body = json.dumps({"template_name": WATI_OTP_TEMPLATE, "broadcast_name": "crayon_app_reset",
                       "parameters": [{"name": "1", "value": code}]}).encode()
    req = urllib.request.Request(f"{WATI_URL}/api/v1/sendTemplateMessage?whatsappNumber={number}", data=body,
                                 method="POST", headers={"Authorization": tok, "Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=15) as r:
            j = json.loads(r.read() or b"{}")
    except urllib.error.HTTPError as e:
        detail = (e.read() or b"").decode("utf-8", "ignore").strip()[:200]
        hint = {401: "WATI_TOKEN is wrong or expired", 403: "WATI_TOKEN is wrong or expired",
                404: "WATI_API_URL is wrong (use the API Endpoint from WATI > API Docs)"}.get(e.code, "")
        raise HTTPException(502, f"WhatsApp code not sent - WATI replied {e.code}" + (f" ({hint})" if hint else "")
                            + (f": {detail}" if detail else ""))
    except Exception as e:
        raise HTTPException(502, f"Could not reach WATI ({type(e).__name__}: {str(e)[:120]}). Check WATI_API_URL.")
    if j.get("result") is False and j.get("validWhatsAppNumber") is not False:
        raise HTTPException(502, "WATI did not send the code: " + str(j.get("info") or j.get("message") or j)[:200])
    if j.get("validWhatsAppNumber") is False:
        raise HTTPException(502, "WhatsApp could not deliver to the mobile number on your account. Ask your admin to check it.")


def _code_hash(uid, code):
    return hmac.new(SECRET.encode(), f"reset:{uid}:{code}".encode(), "sha256").hexdigest()


@app.post("/api/forgot")
async def forgot(request: Request, c=Depends(db)):
    ensure_schema(c)
    email = ((await request.json()).get("email") or "").strip().lower()
    if not email:
        raise HTTPException(400, "Enter your email")
    if not WA_READY:
        raise HTTPException(503, NOT_SET)
    u = c.execute("SELECT id,name,mobile FROM users WHERE email=%s AND active", (email,)).fetchone()
    if not u:
        raise HTTPException(404, "No active user with this email")
    num = wa_number(u["mobile"])
    if not num:
        raise HTTPException(400, "No valid mobile number on your account. Ask your admin to add it in Admin > Users.")
    recent = c.execute("SELECT count(*) n FROM password_resets WHERE user_id=%s AND created_at > now() - interval '15 minutes'",
                       (u["id"],)).fetchone()["n"]
    if recent >= 3:
        raise HTTPException(429, "Too many requests. Please wait 15 minutes and try again.")
    code = f"{secrets.randbelow(1_000_000):06d}"
    send_wa_code(num, code)
    c.execute("UPDATE password_resets SET used=TRUE WHERE user_id=%s AND NOT used", (u["id"],))
    c.execute("INSERT INTO password_resets(user_id,code_hash,expires_at) VALUES(%s,%s,now()+interval '10 minutes')",
              (u["id"], _code_hash(u["id"], code)))
    return {"ok": True, "message": f"A 6-digit code has been sent to your WhatsApp (******{num[-4:]})."}


@app.post("/api/reset")
async def reset_pw(request: Request, c=Depends(db)):
    ensure_schema(c)
    b = await request.json()
    email, code, new = (b.get("email") or "").strip().lower(), (b.get("code") or "").strip(), b.get("password") or ""
    if len(new) < 6:
        raise HTTPException(400, "New password must be at least 6 characters")
    u = c.execute("SELECT id FROM users WHERE email=%s AND active", (email,)).fetchone()
    r = u and c.execute("SELECT * FROM password_resets WHERE user_id=%s AND NOT used AND expires_at>now() "
                        "ORDER BY id DESC LIMIT 1", (u["id"],)).fetchone()
    if not r or r["attempts"] >= 5:
        raise HTTPException(400, "Code expired or not valid. Request a new code.")
    if not hmac.compare_digest(r["code_hash"], _code_hash(u["id"], code)):
        c.execute("UPDATE password_resets SET attempts=attempts+1 WHERE id=%s", (r["id"],))
        raise HTTPException(400, "Wrong code. Please check the WhatsApp message and try again.")
    c.execute("UPDATE password_resets SET used=TRUE WHERE id=%s", (r["id"],))
    c.execute("UPDATE users SET pw_hash=%s WHERE id=%s", (hash_pw(new), u["id"]))
    return {"ok": True}


@app.post("/api/logout")
def logout():
    r = JSONResponse({"ok": True})
    r.delete_cookie(COOKIE)
    return r


@app.get("/api/me")
def me(u=Depends(user)):
    return {"id": u["id"], "name": u["name"], "email": u["email"], "roles": sorted(u["role_set"]), "build": BUILD}


@app.post("/api/me/password")
async def change_pw(request: Request, u=Depends(user), c=Depends(db)):
    b = await request.json()
    row = c.execute("SELECT pw_hash FROM users WHERE id=%s", (u["id"],)).fetchone()
    if not check_pw(b.get("old") or "", row["pw_hash"]):
        raise HTTPException(400, "Current password is wrong")
    if len(b.get("new") or "") < 6:
        raise HTTPException(400, "New password must be at least 6 characters")
    c.execute("UPDATE users SET pw_hash=%s WHERE id=%s", (hash_pw(b["new"]), u["id"]))
    return {"ok": True}


# ---------------------------------------------------------------- masters
CUST_F = ["name", "gstin", "contact_person", "mobile", "email", "address", "city", "state", "credit_limit", "active"]
PROD_F = ["code", "name", "category", "uom", "rate", "gst_pct", "kg_per_rft", "cover_width_ft", "hsn", "active"]


@app.get("/api/customers")
def customers(u=Depends(user), c=Depends(db)):
    return c.execute("SELECT * FROM customers ORDER BY name").fetchall()


@app.post("/api/customers")
@app.put("/api/customers/{rid}")
async def save_customer(request: Request, rid: int = None, u=Depends(user), c=Depends(db)):
    need(u, "sales")
    b = await request.json()
    if not rid and not (b.get("name") or "").strip():
        raise HTTPException(400, "Customer name is required")
    return upsert(c, "customers", CUST_F, b, rid)


@app.get("/api/products")
def products(u=Depends(user), c=Depends(db)):
    return c.execute("SELECT * FROM products ORDER BY category, name").fetchall()


@app.post("/api/products")
@app.put("/api/products/{rid}")
async def save_product(request: Request, rid: int = None, u=Depends(user), c=Depends(db)):
    need(u, "admin")
    return upsert(c, "products", PROD_F, await request.json(), rid)


@app.get("/api/users")
def users(u=Depends(user), c=Depends(db)):
    return c.execute("SELECT id,name,email,mobile,roles,active FROM users ORDER BY name").fetchall()


@app.post("/api/users")
@app.put("/api/users/{rid}")
async def save_user(request: Request, rid: int = None, u=Depends(user), c=Depends(db)):
    need(u, "admin")
    b = await request.json()
    if b.get("email"):
        b["email"] = b["email"].strip().lower()
    if b.get("password"):
        b["pw_hash"] = hash_pw(b["password"])
    elif not rid:
        raise HTTPException(400, "Password is required for a new user")
    row = upsert(c, "users", ["name", "email", "mobile", "roles", "active", "pw_hash"], b, rid)
    row.pop("pw_hash", None)
    return row


@app.get("/api/settings")
def get_settings(u=Depends(user), c=Depends(db)):
    return settings(c)


@app.put("/api/settings")
async def put_settings(request: Request, u=Depends(user), c=Depends(db)):
    need(u, "admin")
    for k, v in (await request.json()).items():
        c.execute("INSERT INTO settings(key,val) VALUES(%s,%s) ON CONFLICT(key) DO UPDATE SET val=EXCLUDED.val", (k, str(v)))
    return settings(c)


# ---------------------------------------------------------------- enquiries
ENQ_F = ["enq_date", "source", "customer_id", "contact_name", "mobile", "site_location", "requirement",
         "owner_id", "status", "follow_up", "lost_reason"]


@app.get("/api/enquiries")
def enquiries(status: str = "", u=Depends(user), c=Depends(db)):
    q = ("SELECT e.*, cu.name customer, us.name owner FROM enquiries e LEFT JOIN customers cu ON cu.id=e.customer_id "
         "LEFT JOIN users us ON us.id=e.owner_id")
    args = ()
    if status:
        q += " WHERE e.status=%s"
        args = (status,)
    return c.execute(q + " ORDER BY e.id DESC LIMIT 500", args).fetchall()


@app.post("/api/enquiries")
@app.put("/api/enquiries/{rid}")
async def save_enquiry(request: Request, rid: int = None, u=Depends(user), c=Depends(db)):
    need(u, "sales")
    b = await request.json()
    if not rid:
        b["enq_no"] = next_no(c, "ENQ")
        b.setdefault("owner_id", u["id"])
        return upsert(c, "enquiries", ENQ_F + ["enq_no"], b)
    return upsert(c, "enquiries", ENQ_F, b, rid)


# ---------------------------------------------------------------- quotations
def calc_line(it, prod):
    L, pcs = D(it.get("length_ft")), D(it.get("pcs"))
    uom = it.get("uom") or (prod or {}).get("uom") or "nos"
    if uom == "rft" and L > 0:
        qty = L * pcs
    elif uom == "kg" and L > 0 and prod and D(prod["kg_per_rft"]) > 0:
        qty = L * pcs * D(prod["kg_per_rft"])
    elif uom == "sqft" and L > 0 and prod and D(prod["cover_width_ft"]) > 0:
        qty = L * pcs * D(prod["cover_width_ft"])
    else:
        qty = D(it.get("qty")) if D(it.get("qty")) > 0 else pcs
    rate = D(it.get("rate")) if it.get("rate") not in (None, "") else D((prod or {}).get("rate"))
    gst = D(it.get("gst_pct")) if it.get("gst_pct") not in (None, "") else D((prod or {}).get("gst_pct", 18))
    qty = qty.quantize(Decimal("0.001"), ROUND_HALF_UP)
    return {"product_id": it.get("product_id") or None, "description": it.get("description") or (prod or {}).get("name"),
            "colour": it.get("colour"), "length_ft": L, "pcs": pcs, "qty": qty, "uom": uom,
            "rate": rate, "gst_pct": gst, "amount": r2(qty * rate)}


def totals(lines, disc_pct, freight):
    sub = sum((l["amount"] for l in lines), Decimal(0))
    disc_pct = D(disc_pct)
    disc = r2(sub * disc_pct / 100)
    gst = r2(sum((l["amount"] * (1 - disc_pct / 100) * l["gst_pct"] / 100 for l in lines), Decimal(0)))
    total = r2(sub - disc + gst + D(freight))
    return {"subtotal": r2(sub), "discount_amt": disc, "gst_amt": gst, "total": total}


def write_items(c, table, fk, pid, lines):
    c.execute(f"DELETE FROM {table} WHERE {fk}=%s", (pid,))
    for i, l in enumerate(lines):
        c.execute(f"INSERT INTO {table}({fk},product_id,description,colour,length_ft,pcs,qty,uom,rate,gst_pct,amount,sort)"
                  " VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                  (pid, l["product_id"], l["description"], l["colour"], l["length_ft"], l["pcs"], l["qty"], l["uom"],
                   l["rate"], l["gst_pct"], l["amount"], i))


def build_lines(c, items):
    prods = {p["id"]: p for p in c.execute("SELECT * FROM products")}
    lines = [calc_line(it, prods.get(int(it["product_id"])) if it.get("product_id") else None) for it in items or []]
    lines = [l for l in lines if l["qty"] > 0 or l["description"]]
    if not lines:
        raise HTTPException(400, "Add at least one item")
    return lines


@app.get("/api/quotes")
def quotes(status: str = "", u=Depends(user), c=Depends(db)):
    q = ("SELECT q.*, cu.name customer, e.enq_no FROM quotes q JOIN customers cu ON cu.id=q.customer_id "
         "LEFT JOIN enquiries e ON e.id=q.enquiry_id")
    args = ()
    if status:
        q += " WHERE q.status=%s"
        args = (status,)
    return c.execute(q + " ORDER BY q.id DESC LIMIT 500", args).fetchall()


@app.get("/api/quotes/{rid}")
def quote(rid: int, u=Depends(user), c=Depends(db)):
    q = c.execute("SELECT q.*, cu.name customer FROM quotes q JOIN customers cu ON cu.id=q.customer_id WHERE q.id=%s",
                  (rid,)).fetchone()
    if not q:
        raise HTTPException(404, "Quote not found")
    q["items"] = c.execute("SELECT * FROM quote_items WHERE quote_id=%s ORDER BY sort", (rid,)).fetchall()
    return q


@app.post("/api/quotes")
@app.put("/api/quotes/{rid}")
async def save_quote(request: Request, rid: int = None, u=Depends(user), c=Depends(db)):
    need(u, "sales")
    b = await request.json()
    if not b.get("customer_id"):
        raise HTTPException(400, "Select a customer")
    with c.transaction():
        lines = build_lines(c, b.get("items"))
        t = totals(lines, b.get("discount_pct"), b.get("freight"))
        head = {**clean(b, ["customer_id", "enquiry_id", "quote_date", "valid_till", "discount_pct", "freight", "terms", "notes"]), **t}
        if rid:
            cur = c.execute("SELECT status FROM quotes WHERE id=%s", (rid,)).fetchone()
            if not cur:
                raise HTTPException(404, "Quote not found")
            if cur["status"] not in ("Draft", "Sent"):
                raise HTTPException(400, f"A {cur['status']} quote cannot be edited - create a revision")
            row = upsert(c, "quotes", list(head), head, rid)
        else:
            s = settings(c)
            if not head.get("valid_till"):
                head["valid_till"] = str(dt.date.today() + dt.timedelta(days=int(s.get("quote_validity_days") or 7)))
            if not head.get("terms"):
                head["terms"] = s.get("quote_terms")
            head.update(quote_no=next_no(c, "QT"), rev=0, created_by=u["id"])
            row = upsert(c, "quotes", list(head), head)
            if b.get("enquiry_id"):
                c.execute("UPDATE enquiries SET status='Quoted' WHERE id=%s AND status='Open'", (b["enquiry_id"],))
        write_items(c, "quote_items", "quote_id", row["id"], lines)
    return quote(row["id"], u, c)


@app.post("/api/quotes/{rid}/revise")
def revise_quote(rid: int, u=Depends(user), c=Depends(db)):
    need(u, "sales")
    with c.transaction():
        q = quote(rid, u, c)
        maxrev = c.execute("SELECT max(rev) m FROM quotes WHERE quote_no=%s", (q["quote_no"],)).fetchone()["m"]
        new = c.execute(
            "INSERT INTO quotes(quote_no,rev,enquiry_id,customer_id,valid_till,discount_pct,freight,subtotal,discount_amt,"
            "gst_amt,total,terms,notes,status,created_by) SELECT quote_no,%s,enquiry_id,customer_id,valid_till,discount_pct,"
            "freight,subtotal,discount_amt,gst_amt,total,terms,notes,'Draft',%s FROM quotes WHERE id=%s RETURNING id",
            (maxrev + 1, u["id"], rid)).fetchone()["id"]
        c.execute("INSERT INTO quote_items(quote_id,product_id,description,colour,length_ft,pcs,qty,uom,rate,gst_pct,amount,sort)"
                  " SELECT %s,product_id,description,colour,length_ft,pcs,qty,uom,rate,gst_pct,amount,sort FROM quote_items"
                  " WHERE quote_id=%s", (new, rid))
        c.execute("UPDATE quotes SET status='Revised' WHERE id=%s AND status IN ('Draft','Sent')", (rid,))
    return quote(new, u, c)


@app.post("/api/quotes/{rid}/status")
async def quote_status(rid: int, request: Request, u=Depends(user), c=Depends(db)):
    need(u, "sales")
    st = (await request.json()).get("status")
    if st not in ("Draft", "Sent", "Rejected"):
        raise HTTPException(400, "Invalid status")
    c.execute("UPDATE quotes SET status=%s WHERE id=%s", (st, rid))
    if st == "Rejected":
        c.execute("UPDATE enquiries SET status='Lost' WHERE id=(SELECT enquiry_id FROM quotes WHERE id=%s)", (rid,))
    return {"ok": True}


# ---------------------------------------------------------------- sales orders
def approval_reasons(c, q, advance):
    s = settings(c)
    why = []
    if D(q["discount_pct"]) > D(s.get("max_discount_pct") or 0):
        why.append(f"Discount {q['discount_pct']}% is above the {s.get('max_discount_pct')}% limit")
    cust = c.execute("SELECT credit_limit FROM customers WHERE id=%s", (q["customer_id"],)).fetchone()
    open_bal = c.execute("SELECT coalesce(sum(total-advance),0) b FROM orders WHERE customer_id=%s AND status NOT IN "
                         "('Rejected','Closed','Dispatched')", (q["customer_id"],)).fetchone()["b"]
    exposure = D(open_bal) + D(q["total"]) - D(advance)
    limit = D(cust["credit_limit"])
    if exposure > limit:
        why.append(f"Credit exposure Rs {exposure:,.0f} is above the credit limit Rs {limit:,.0f}")
    return why


@app.post("/api/quotes/{rid}/convert")
async def convert(rid: int, request: Request, u=Depends(user), c=Depends(db)):
    need(u, "sales")
    b = await request.json()
    with c.transaction():
        q = quote(rid, u, c)
        if q["status"] not in ("Draft", "Sent"):
            raise HTTPException(400, f"This quote is {q['status']} and cannot be converted")
        why = approval_reasons(c, q, b.get("advance"))
        st = "Pending Approval" if why else "Approved"
        o = c.execute("INSERT INTO orders(so_no,quote_id,customer_id,delivery_date,customer_po,total,advance,status,"
                      "approval_reason,site_address,notes,created_by) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) RETURNING id",
                      (next_no(c, "SO"), rid, q["customer_id"], b.get("delivery_date") or None, b.get("customer_po"),
                       q["total"], D(b.get("advance")), st, "; ".join(why) or None, b.get("site_address"), b.get("notes"),
                       u["id"])).fetchone()["id"]
        c.execute("INSERT INTO order_items(order_id,product_id,description,colour,length_ft,pcs,qty,uom,rate,gst_pct,amount,sort)"
                  " SELECT %s,product_id,description,colour,length_ft,pcs,qty,uom,rate,gst_pct,amount,sort FROM quote_items"
                  " WHERE quote_id=%s", (o, rid))
        c.execute("UPDATE quotes SET status='Accepted' WHERE id=%s", (rid,))
        if q["enquiry_id"]:
            c.execute("UPDATE enquiries SET status='Won' WHERE id=%s", (q["enquiry_id"],))
    return order(o, u, c)


@app.get("/api/orders")
def orders(status: str = "", u=Depends(user), c=Depends(db)):
    q = ("SELECT o.*, cu.name customer, qt.quote_no, qt.rev FROM orders o JOIN customers cu ON cu.id=o.customer_id "
         "LEFT JOIN quotes qt ON qt.id=o.quote_id")
    args = ()
    if status:
        q += " WHERE o.status=%s"
        args = (status,)
    return c.execute(q + " ORDER BY o.id DESC LIMIT 500", args).fetchall()


@app.get("/api/orders/{rid}")
def order(rid: int, u=Depends(user), c=Depends(db)):
    o = c.execute("SELECT o.*, cu.name customer, cu.mobile customer_mobile, ap.name approved_by_name FROM orders o "
                  "JOIN customers cu ON cu.id=o.customer_id LEFT JOIN users ap ON ap.id=o.approved_by WHERE o.id=%s",
                  (rid,)).fetchone()
    if not o:
        raise HTTPException(404, "Order not found")
    o["items"] = c.execute(
        "SELECT oi.*, coalesce((SELECT sum(done_pcs) FROM cutting WHERE order_item_id=oi.id),0) cut_pcs,"
        " coalesce((SELECT sum(di.pcs) FROM dispatch_items di WHERE di.order_item_id=oi.id),0) dispatched_pcs"
        " FROM order_items oi WHERE order_id=%s ORDER BY sort", (rid,)).fetchall()
    o["cutting"] = c.execute("SELECT * FROM cutting WHERE order_id=%s ORDER BY id", (rid,)).fetchall()
    o["dispatches"] = c.execute("SELECT * FROM dispatches WHERE order_id=%s ORDER BY id", (rid,)).fetchall()
    return o


@app.post("/api/orders/{rid}/approve")
async def approve(rid: int, request: Request, u=Depends(user), c=Depends(db)):
    need(u, "approver")
    b = await request.json()
    st = "Approved" if b.get("decision") == "approve" else "Rejected"
    row = c.execute("UPDATE orders SET status=%s, approved_by=%s, approved_at=now(), approval_note=%s "
                    "WHERE id=%s AND status='Pending Approval' RETURNING id", (st, u["id"], b.get("note"), rid)).fetchone()
    if not row:
        raise HTTPException(400, "This order is not waiting for approval")
    return order(rid, u, c)


@app.put("/api/orders/{rid}")
async def update_order(rid: int, request: Request, u=Depends(user), c=Depends(db)):
    need(u, "sales")
    return upsert(c, "orders", ["delivery_date", "customer_po", "advance", "site_address", "notes"], await request.json(), rid)


@app.post("/api/orders/{rid}/status")
async def order_status(rid: int, request: Request, u=Depends(user), c=Depends(db)):
    need(u, "sales", "production", "dispatch")
    st = (await request.json()).get("status")
    if st not in ("Approved", "In Production", "Ready", "Dispatched", "Closed"):
        raise HTTPException(400, "Invalid status")
    cur = c.execute("SELECT status FROM orders WHERE id=%s", (rid,)).fetchone()
    if not cur or cur["status"] in ("Pending Approval", "Rejected"):
        raise HTTPException(400, "Order must be approved first")
    c.execute("UPDATE orders SET status=%s WHERE id=%s", (st, rid))
    return {"ok": True}


# ---------------------------------------------------------------- cutting program
@app.get("/api/cutting")
def cutting_board(status: str = "", u=Depends(user), c=Depends(db)):
    q = ("SELECT ct.*, o.so_no, cu.name customer, oi.description, oi.colour FROM cutting ct JOIN orders o ON o.id=ct.order_id "
         "JOIN customers cu ON cu.id=o.customer_id LEFT JOIN order_items oi ON oi.id=ct.order_item_id")
    args = ()
    if status:
        q += " WHERE ct.status=%s"
        args = (status,)
    return c.execute(q + " ORDER BY ct.plan_date, ct.id LIMIT 1000", args).fetchall()


@app.post("/api/orders/{rid}/cutting")
async def plan_cutting(rid: int, request: Request, u=Depends(user), c=Depends(db)):
    """Create cutting lines. Body {lines:[{order_item_id,length_ft,pcs,coil_no,machine,plan_date}]} or {auto:true}."""
    need(u, "production")
    b = await request.json()
    o = c.execute("SELECT status FROM orders WHERE id=%s", (rid,)).fetchone()
    if not o or o["status"] in ("Pending Approval", "Rejected"):
        raise HTTPException(400, "Order must be approved before planning")
    with c.transaction():
        lines = b.get("lines") or []
        if b.get("auto"):
            lines = [{"order_item_id": r["id"], "length_ft": r["length_ft"], "pcs": r["pcs"]} for r in c.execute(
                "SELECT oi.* FROM order_items oi LEFT JOIN products p ON p.id=oi.product_id WHERE oi.order_id=%s "
                "AND coalesce(oi.length_ft,0)>0 AND NOT EXISTS (SELECT 1 FROM cutting ct WHERE ct.order_item_id=oi.id)",
                (rid,))]
        for l in lines:
            c.execute("INSERT INTO cutting(order_id,order_item_id,length_ft,pcs,coil_no,machine,plan_date) "
                      "VALUES(%s,%s,%s,%s,%s,%s,coalesce(%s,CURRENT_DATE))",
                      (rid, l.get("order_item_id"), D(l.get("length_ft")), D(l.get("pcs")), l.get("coil_no"),
                       l.get("machine"), l.get("plan_date") or None))
        if lines:
            c.execute("UPDATE orders SET status='In Production' WHERE id=%s AND status='Approved'", (rid,))
    return order(rid, u, c)


@app.put("/api/cutting/{cid}")
async def update_cutting(cid: int, request: Request, u=Depends(user), c=Depends(db)):
    need(u, "production")
    b = await request.json()
    if b.get("status") == "Done" and "done_pcs" not in b:
        b["done_pcs"] = c.execute("SELECT pcs FROM cutting WHERE id=%s", (cid,)).fetchone()["pcs"]
    row = upsert(c, "cutting", ["length_ft", "pcs", "coil_no", "machine", "plan_date", "status", "done_pcs", "remarks"], b, cid)
    c.execute("UPDATE cutting SET updated_at=now() WHERE id=%s", (cid,))
    pend = c.execute("SELECT count(*) n FROM cutting WHERE order_id=%s AND status<>'Done'", (row["order_id"],)).fetchone()["n"]
    if pend == 0:
        c.execute("UPDATE orders SET status='Ready' WHERE id=%s AND status='In Production'", (row["order_id"],))
    return row


@app.delete("/api/cutting/{cid}")
def delete_cutting(cid: int, u=Depends(user), c=Depends(db)):
    need(u, "production")
    c.execute("DELETE FROM cutting WHERE id=%s AND status='Planned'", (cid,))
    return {"ok": True}


# ---------------------------------------------------------------- dispatch
DISP_F = ["dc_date", "vehicle_no", "driver_name", "driver_mobile", "transporter", "invoice_no", "eway_bill_no",
          "status", "delivered_on", "pod_note", "remarks"]


@app.get("/api/dispatches")
def dispatches(status: str = "", u=Depends(user), c=Depends(db)):
    q = ("SELECT d.*, o.so_no, cu.name customer FROM dispatches d JOIN orders o ON o.id=d.order_id "
         "JOIN customers cu ON cu.id=o.customer_id")
    args = ()
    if status:
        q += " WHERE d.status=%s"
        args = (status,)
    return c.execute(q + " ORDER BY d.id DESC LIMIT 500", args).fetchall()


@app.get("/api/dispatches/{rid}")
def dispatch(rid: int, u=Depends(user), c=Depends(db)):
    d = c.execute("SELECT d.*, o.so_no, o.customer_po, o.site_address, cu.name customer, cu.gstin, cu.address, cu.city, "
                  "cu.mobile customer_mobile FROM dispatches d JOIN orders o ON o.id=d.order_id "
                  "JOIN customers cu ON cu.id=o.customer_id WHERE d.id=%s", (rid,)).fetchone()
    if not d:
        raise HTTPException(404, "Dispatch not found")
    d["items"] = c.execute("SELECT di.*, oi.description, oi.colour, oi.length_ft, oi.uom FROM dispatch_items di "
                           "JOIN order_items oi ON oi.id=di.order_item_id WHERE di.dispatch_id=%s ORDER BY oi.sort",
                           (rid,)).fetchall()
    return d


@app.post("/api/orders/{rid}/dispatch")
async def create_dispatch(rid: int, request: Request, u=Depends(user), c=Depends(db)):
    """Body: header fields + items:[{order_item_id, pcs}] (pcs omitted = full balance)."""
    need(u, "dispatch")
    b = await request.json()
    with c.transaction():
        o = order(rid, u, c)
        if o["status"] in ("Pending Approval", "Rejected", "Closed"):
            raise HTTPException(400, f"Order is {o['status']} - cannot dispatch")
        if not (b.get("vehicle_no") or "").strip():
            raise HTTPException(400, "Vehicle number is required")
        want = {int(i["order_item_id"]): i.get("pcs") for i in b.get("items") or []}
        lines = []
        for it in o["items"]:
            bal = D(it["pcs"]) - D(it["dispatched_pcs"])
            if want and it["id"] not in want:
                continue
            pcs = D(want.get(it["id"])) if want.get(it["id"]) not in (None, "") else bal
            if pcs <= 0:
                continue
            if pcs > bal:
                raise HTTPException(400, f"{it['description']}: only {bal} pcs left to dispatch")
            qty = (D(it["qty"]) * pcs / D(it["pcs"])) if D(it["pcs"]) > 0 else D(it["qty"])
            lines.append((it["id"], pcs, qty.quantize(Decimal("0.001"))))
        if not lines:
            raise HTTPException(400, "Nothing left to dispatch on this order")
        b["dc_no"] = next_no(c, "DC")
        b["order_id"] = rid
        b["created_by"] = u["id"]
        d = upsert(c, "dispatches", DISP_F + ["dc_no", "order_id", "created_by"], b)
        for oid, pcs, qty in lines:
            c.execute("INSERT INTO dispatch_items(dispatch_id,order_item_id,pcs,qty) VALUES(%s,%s,%s,%s)", (d["id"], oid, pcs, qty))
        left = c.execute("SELECT coalesce(sum(oi.pcs),0) - coalesce((SELECT sum(di.pcs) FROM dispatch_items di JOIN order_items x "
                         "ON x.id=di.order_item_id WHERE x.order_id=%s),0) b FROM order_items oi WHERE oi.order_id=%s",
                         (rid, rid)).fetchone()["b"]
        if D(left) <= 0:
            c.execute("UPDATE orders SET status='Dispatched' WHERE id=%s", (rid,))
    return dispatch(d["id"], u, c)


@app.put("/api/dispatches/{rid}")
async def update_dispatch(rid: int, request: Request, u=Depends(user), c=Depends(db)):
    need(u, "dispatch")
    return upsert(c, "dispatches", DISP_F, await request.json(), rid)


# ---------------------------------------------------------------- dashboard
# ---------------------------------------------------------------- sales: leads & visits
LEAD_F = ["lead_date", "company", "contact_person", "designation", "mobile", "alt_mobile", "email", "gstin", "address",
          "area", "city", "district", "pincode", "customer_type", "source", "referred_by", "products", "requirement",
          "application", "approx_qty", "expected_value", "expected_close", "competitor", "priority", "status",
          "owner_id", "next_follow_up", "lost_reason", "remarks"]
VISIT_F = ["visit_date", "lead_id", "customer_id", "party_name", "visit_type", "purpose", "location", "latitude",
           "longitude", "person_met", "person_designation", "person_mobile", "discussion", "outcome", "order_potential",
           "next_action", "next_follow_up", "photo"]


@app.get("/api/leads")
def leads(status: str = "", mine: int = 0, u=Depends(user), c=Depends(db)):
    q = ("SELECT l.*, us.name owner, (SELECT count(*) FROM visits v WHERE v.lead_id=l.id) visits, "
         "(SELECT max(visit_date) FROM visits v WHERE v.lead_id=l.id) last_visit, e.enq_no "
         "FROM leads l LEFT JOIN users us ON us.id=l.owner_id LEFT JOIN enquiries e ON e.id=l.enquiry_id WHERE TRUE")
    args = []
    if status:
        q += " AND l.status=%s"
        args.append(status)
    if mine:
        q += " AND l.owner_id=%s"
        args.append(u["id"])
    return c.execute(q + " ORDER BY l.id DESC LIMIT 1000", args).fetchall()


@app.post("/api/leads")
@app.put("/api/leads/{rid}")
async def save_lead(request: Request, rid: int = None, u=Depends(user), c=Depends(db)):
    need(u, "sales")
    b = await request.json()
    if not rid and not (b.get("company") or "").strip():
        raise HTTPException(400, "Company / customer name is required")
    if not rid and not (b.get("mobile") or "").strip():
        raise HTTPException(400, "Mobile number is required")
    if not rid:
        b.update(lead_no=next_no(c, "LD"), created_by=u["id"])
        b.setdefault("owner_id", u["id"])
        return upsert(c, "leads", LEAD_F + ["lead_no", "created_by"], b)
    row = upsert(c, "leads", LEAD_F, b, rid)
    c.execute("UPDATE leads SET updated_at=now() WHERE id=%s", (rid,))
    return row


@app.post("/api/leads/{rid}/convert")
def convert_lead(rid: int, u=Depends(user), c=Depends(db)):
    """Lead -> customer (created if needed) + enquiry."""
    need(u, "sales")
    with c.transaction():
        l = c.execute("SELECT * FROM leads WHERE id=%s", (rid,)).fetchone()
        if not l:
            raise HTTPException(404, "Lead not found")
        if l["enquiry_id"]:
            raise HTTPException(400, "This lead is already converted")
        cid = l["customer_id"]
        if not cid:
            hit = c.execute("SELECT id FROM customers WHERE lower(name)=lower(%s) LIMIT 1", (l["company"],)).fetchone()
            cid = hit["id"] if hit else c.execute(
                "INSERT INTO customers(name,gstin,contact_person,mobile,email,address,city) VALUES(%s,%s,%s,%s,%s,%s,%s) RETURNING id",
                (l["company"], l["gstin"], l["contact_person"], l["mobile"], l["email"],
                 ", ".join(x for x in [l["address"], l["area"], l["district"], l["pincode"]] if x), l["city"])).fetchone()["id"]
        req = "; ".join(x for x in [l["products"] and "Products: " + l["products"], l["requirement"],
                                    l["approx_qty"] and "Qty: " + l["approx_qty"], l["application"] and "Use: " + l["application"]] if x)
        e = c.execute("INSERT INTO enquiries(enq_no,source,customer_id,contact_name,mobile,site_location,requirement,owner_id,"
                      "follow_up) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s) RETURNING id, enq_no",
                      (next_no(c, "ENQ"), l["source"], cid, l["contact_person"], l["mobile"],
                       l["area"] or l["city"], req or None, l["owner_id"] or u["id"], l["next_follow_up"])).fetchone()
        c.execute("UPDATE leads SET status='Converted', customer_id=%s, enquiry_id=%s, updated_at=now() WHERE id=%s",
                  (cid, e["id"], rid))
    return {"ok": True, "enq_no": e["enq_no"], "enquiry_id": e["id"], "customer_id": cid}


@app.get("/api/visits")
def visits(mine: int = 0, lead_id: int = 0, days: int = 0, u=Depends(user), c=Depends(db)):
    q = ("SELECT v.id, v.visit_no, v.visit_date, v.check_in, v.lead_id, v.customer_id, v.party_name, v.visit_type, v.purpose,"
         " v.location, v.latitude, v.longitude, v.person_met, v.person_designation, v.person_mobile, v.discussion, v.outcome,"
         " v.order_potential, v.next_action, v.next_follow_up, (v.photo IS NOT NULL) has_photo, v.user_id,"
         " us.name salesperson, coalesce(l.company, cu.name, v.party_name) party, l.lead_no "
         "FROM visits v LEFT JOIN users us ON us.id=v.user_id LEFT JOIN leads l ON l.id=v.lead_id "
         "LEFT JOIN customers cu ON cu.id=v.customer_id WHERE TRUE")
    args = []
    if mine:
        q += " AND v.user_id=%s"
        args.append(u["id"])
    if lead_id:
        q += " AND v.lead_id=%s"
        args.append(lead_id)
    if days:
        q += " AND v.visit_date >= CURRENT_DATE - (%s - 1)"
        args.append(days)
    return c.execute(q + " ORDER BY v.check_in DESC LIMIT 1000", args).fetchall()


@app.get("/api/visits/{rid}/photo")
def visit_photo(rid: int, u=Depends(user), c=Depends(db)):
    r = c.execute("SELECT photo FROM visits WHERE id=%s", (rid,)).fetchone()
    if not r or not r["photo"]:
        raise HTTPException(404, "No photo")
    return {"photo": r["photo"]}


@app.post("/api/visits")
async def save_visit(request: Request, u=Depends(user), c=Depends(db)):
    need(u, "sales")
    b = await request.json()
    for k in ("lead_id", "customer_id"):
        b[k] = int(b[k]) if str(b.get(k) or "").isdigit() else None
    if not (b["lead_id"] or b["customer_id"] or (b.get("party_name") or "").strip()):
        raise HTTPException(400, "Choose a lead or customer, or type the party name")
    if not (b.get("discussion") or "").strip():
        raise HTTPException(400, "Enter what was discussed")
    if b.get("photo") and len(b["photo"]) > 700_000:
        raise HTTPException(400, "Photo is too large")
    with c.transaction():
        b.update(visit_no=next_no(c, "VS"), user_id=u["id"])
        row = upsert(c, "visits", VISIT_F + ["visit_no", "user_id"], b)
        if b["lead_id"]:
            st = {"Order confirmed": "Qualified", "Quote requested": "Qualified", "Not interested": "Lost"}.get(b.get("outcome"))
            c.execute("UPDATE leads SET next_follow_up=coalesce(%s, next_follow_up), updated_at=now(), "
                      "status=CASE WHEN status IN ('Converted','Lost') THEN status WHEN %s::text IS NOT NULL THEN %s "
                      "WHEN status='New' THEN 'Contacted' ELSE status END WHERE id=%s",
                      (b.get("next_follow_up") or None, st, st, b["lead_id"]))
    if b.get("latitude") and b.get("longitude"):
        t = active_trip(c, u["id"])
        if t:
            add_point(c, t["id"], b["latitude"], b["longitude"], b.get("accuracy"), "visit", row["id"],
                      (b.get("party_name") or "").strip() or None, force=True)
    row.pop("photo", None)
    return row


# ---------------------------------------------------------------- sales: travel / distance tracking
def km_between(a1, o1, a2, o2):
    a1, o1, a2, o2 = map(lambda x: math.radians(float(x)), (a1, o1, a2, o2))
    h = math.sin((a2 - a1) / 2) ** 2 + math.cos(a1) * math.cos(a2) * math.sin((o2 - o1) / 2) ** 2
    return 6371.0 * 2 * math.asin(min(1.0, math.sqrt(h)))


def active_trip(c, uid):
    return c.execute("SELECT * FROM trips WHERE user_id=%s AND status='Active' ORDER BY id DESC LIMIT 1", (uid,)).fetchone()


def recalc_km(c, tid):
    pts = c.execute("SELECT lat,lng FROM trip_points WHERE trip_id=%s ORDER BY ts, id", (tid,)).fetchall()
    km = sum(km_between(p["lat"], p["lng"], q["lat"], q["lng"]) for p, q in zip(pts, pts[1:]))
    c.execute("UPDATE trips SET gps_km=%s WHERE id=%s", (round(km, 2), tid))
    return round(km, 2)


def add_point(c, tid, lat, lng, acc=None, kind="track", visit_id=None, label=None, force=False):
    """Store a GPS point. Track pings are skipped when inaccurate (>150 m), too close to the last point (<100 m),
    or an impossible jump (>150 km/h) so GPS noise doesn't inflate the distance."""
    lat, lng = float(lat), float(lng)
    if not (-90 <= lat <= 90 and -180 <= lng <= 180):
        raise HTTPException(400, "Bad GPS position")
    if not force:
        if acc not in (None, "") and float(acc) > 150:
            return {"skipped": "low accuracy"}
        last = c.execute("SELECT lat,lng,ts FROM trip_points WHERE trip_id=%s ORDER BY ts DESC, id DESC LIMIT 1", (tid,)).fetchone()
        if last:
            d = km_between(last["lat"], last["lng"], lat, lng)
            if d < 0.1:
                return {"skipped": "not moved"}
            hrs = max((dt.datetime.now(dt.timezone.utc) - last["ts"]).total_seconds() / 3600, 1 / 3600)
            if d / hrs > 150:
                return {"skipped": "jump"}
    c.execute("INSERT INTO trip_points(trip_id,lat,lng,accuracy,kind,visit_id,label) VALUES(%s,%s,%s,%s,%s,%s,%s)",
              (tid, lat, lng, acc if acc not in ("",) else None, kind, visit_id, label))
    return {"ok": True, "km": recalc_km(c, tid)}


def need_gps(b):
    if b.get("lat") in (None, "") or b.get("lng") in (None, ""):
        raise HTTPException(400, "GPS location is needed. Allow location access for this site and try again.")


@app.get("/api/trips/active")
def trip_active(u=Depends(user), c=Depends(db)):
    t = active_trip(c, u["id"])
    if t:
        t["points"] = c.execute("SELECT count(*) n FROM trip_points WHERE trip_id=%s", (t["id"],)).fetchone()["n"]
    return {"trip": t}


@app.post("/api/trips/start")
async def trip_start(request: Request, u=Depends(user), c=Depends(db)):
    need(u, "sales")
    b = await request.json()
    need_gps(b)
    if active_trip(c, u["id"]):
        raise HTTPException(400, "You already have a trip running. End it first.")
    with c.transaction():
        t = c.execute("INSERT INTO trips(trip_no,user_id,start_lat,start_lng,start_place,start_odo,vehicle) "
                      "VALUES(%s,%s,%s,%s,%s,%s,%s) RETURNING *",
                      (next_no(c, "TR"), u["id"], b["lat"], b["lng"], b.get("place") or None,
                       b.get("odo") or None, b.get("vehicle") or None)).fetchone()
        add_point(c, t["id"], b["lat"], b["lng"], b.get("accuracy"), "start", label=b.get("place") or "Start", force=True)
    return trip_active(u, c)


@app.post("/api/trips/point")
async def trip_point(request: Request, u=Depends(user), c=Depends(db)):
    b = await request.json()
    need_gps(b)
    t = active_trip(c, u["id"])
    if not t:
        return {"skipped": "no active trip"}
    return add_point(c, t["id"], b["lat"], b["lng"], b.get("accuracy"))


@app.post("/api/trips/end")
async def trip_end(request: Request, u=Depends(user), c=Depends(db)):
    b = await request.json()
    t = active_trip(c, u["id"])
    if not t:
        raise HTTPException(400, "No trip is running")
    if b.get("odo") and t["start_odo"] and D(b["odo"]) < D(t["start_odo"]):
        raise HTTPException(400, "End odometer reading is less than the start reading")
    with c.transaction():
        if b.get("lat") not in (None, "") and b.get("lng") not in (None, ""):
            add_point(c, t["id"], b["lat"], b["lng"], b.get("accuracy"), "end", label=b.get("place") or "End", force=True)
        km = recalc_km(c, t["id"])
        c.execute("UPDATE trips SET status='Closed', ended_at=now(), end_lat=%s, end_lng=%s, end_place=%s, end_odo=%s, "
                  "remarks=%s WHERE id=%s", (b.get("lat") or None, b.get("lng") or None, b.get("place") or None,
                                            b.get("odo") or None, b.get("remarks") or None, t["id"]))
    return {"ok": True, "km": km, "trip_no": t["trip_no"]}


TRIP_LIST = ("SELECT t.*, us.name salesperson, (SELECT count(*) FROM trip_points p WHERE p.trip_id=t.id AND p.kind='visit') visits, "
             "(SELECT count(*) FROM trip_points p WHERE p.trip_id=t.id) points, "
             "CASE WHEN t.end_odo IS NOT NULL AND t.start_odo IS NOT NULL THEN t.end_odo-t.start_odo END odo_km, "
             "round(extract(epoch FROM coalesce(t.ended_at,now())-t.started_at)/3600.0,2) hours "
             "FROM trips t LEFT JOIN users us ON us.id=t.user_id WHERE TRUE")


@app.get("/api/trips")
def trips(days: int = 30, mine: int = 0, user_id: int = 0, u=Depends(user), c=Depends(db)):
    q, args = TRIP_LIST, []
    if days:
        q += " AND t.trip_date >= CURRENT_DATE - (%s - 1)"
        args.append(days)
    if mine:
        q += " AND t.user_id=%s"
        args.append(u["id"])
    elif user_id:
        q += " AND t.user_id=%s"
        args.append(user_id)
    return c.execute(q + " ORDER BY t.started_at DESC LIMIT 1000", args).fetchall()


@app.get("/api/trips/{rid}")
def trip(rid: int, u=Depends(user), c=Depends(db)):
    t = c.execute(TRIP_LIST + " AND t.id=%s", (rid,)).fetchone()
    if not t:
        raise HTTPException(404, "Trip not found")
    t["points"] = c.execute("SELECT p.ts, p.lat, p.lng, p.accuracy, p.kind, p.label, p.visit_id, "
                            "coalesce(l.company, cu.name, v.party_name, p.label) party, v.visit_no, v.outcome "
                            "FROM trip_points p LEFT JOIN visits v ON v.id=p.visit_id LEFT JOIN leads l ON l.id=v.lead_id "
                            "LEFT JOIN customers cu ON cu.id=v.customer_id WHERE p.trip_id=%s ORDER BY p.ts, p.id", (rid,)).fetchall()
    return t


@app.get("/api/sales-dashboard")
def sales_dashboard(u=Depends(user), c=Depends(db)):
    one = lambda sql, *a: c.execute(sql, a).fetchone()["n"]
    open_st = "('New','Contacted','Visit Planned','Qualified')"
    return {
        "leads_month": one("SELECT count(*) n FROM leads WHERE date_trunc('month',lead_date)=date_trunc('month',CURRENT_DATE)"),
        "open_leads": one(f"SELECT count(*) n FROM leads WHERE status IN {open_st}"),
        "hot_leads": one(f"SELECT count(*) n FROM leads WHERE priority='Hot' AND status IN {open_st}"),
        "pipeline": one(f"SELECT coalesce(sum(expected_value),0) n FROM leads WHERE status IN {open_st}"),
        "followups_due": one(f"SELECT count(*) n FROM leads WHERE status IN {open_st} AND next_follow_up<=CURRENT_DATE"),
        "visits_today": one("SELECT count(*) n FROM visits WHERE visit_date=CURRENT_DATE"),
        "visits_month": one("SELECT count(*) n FROM visits WHERE date_trunc('month',visit_date)=date_trunc('month',CURRENT_DATE)"),
        "converted_month": one("SELECT count(*) n FROM leads WHERE status='Converted' AND date_trunc('month',updated_at)=date_trunc('month',now())"),
        "km_today": one("SELECT coalesce(sum(gps_km),0) n FROM trips WHERE trip_date=CURRENT_DATE"),
        "km_month": one("SELECT coalesce(sum(gps_km),0) n FROM trips WHERE date_trunc('month',trip_date)=date_trunc('month',CURRENT_DATE)"),
        "active_trips": one("SELECT count(*) n FROM trips WHERE status='Active'"),
        "trips_today": c.execute(TRIP_LIST + " AND t.trip_date=CURRENT_DATE ORDER BY t.started_at").fetchall(),
        "due": c.execute(f"SELECT l.id, l.lead_no, l.company, l.mobile, l.priority, l.status, l.next_follow_up, us.name owner "
                         f"FROM leads l LEFT JOIN users us ON us.id=l.owner_id WHERE l.status IN {open_st} "
                         f"AND l.next_follow_up<=CURRENT_DATE ORDER BY l.next_follow_up, l.id LIMIT 50").fetchall(),
        "by_person": c.execute(
            "SELECT us.name, (SELECT count(*) FROM leads l WHERE l.owner_id=us.id AND date_trunc('month',l.lead_date)=date_trunc('month',CURRENT_DATE)) leads,"
            " (SELECT count(*) FROM visits v WHERE v.user_id=us.id AND date_trunc('month',v.visit_date)=date_trunc('month',CURRENT_DATE)) visits,"
            " (SELECT count(*) FROM leads l WHERE l.owner_id=us.id AND l.status='Converted' AND date_trunc('month',l.updated_at)=date_trunc('month',now())) converted,"
            " (SELECT coalesce(sum(t.gps_km),0) FROM trips t WHERE t.user_id=us.id AND date_trunc('month',t.trip_date)=date_trunc('month',CURRENT_DATE)) km,"
            " (SELECT count(*) FROM trips t WHERE t.user_id=us.id AND date_trunc('month',t.trip_date)=date_trunc('month',CURRENT_DATE)) trips"
            " FROM users us WHERE us.active ORDER BY km DESC, visits DESC, leads DESC").fetchall(),
    }


@app.get("/api/dashboard")
def dashboard(u=Depends(user), c=Depends(db)):
    one = lambda sql: c.execute(sql).fetchone()["n"]
    return {
        "open_enquiries": one("SELECT count(*) n FROM enquiries WHERE status IN ('Open','Quoted')"),
        "followups_due": one("SELECT count(*) n FROM enquiries WHERE status IN ('Open','Quoted') AND follow_up<=CURRENT_DATE"),
        "quotes_open": one("SELECT count(*) n FROM quotes WHERE status IN ('Draft','Sent')"),
        "quotes_value": one("SELECT coalesce(sum(total),0) n FROM quotes WHERE status IN ('Draft','Sent')"),
        "pending_approval": one("SELECT count(*) n FROM orders WHERE status='Pending Approval'"),
        "in_production": one("SELECT count(*) n FROM orders WHERE status IN ('Approved','In Production')"),
        "ready": one("SELECT count(*) n FROM orders WHERE status='Ready'"),
        "month_orders": one("SELECT coalesce(sum(total),0) n FROM orders WHERE status<>'Rejected' "
                            "AND date_trunc('month',so_date)=date_trunc('month',CURRENT_DATE)"),
        "month_dispatches": one("SELECT count(*) n FROM dispatches WHERE date_trunc('month',dc_date)=date_trunc('month',CURRENT_DATE)"),
        "cutting_pending": one("SELECT count(*) n FROM cutting WHERE status<>'Done'"),
    }


# ---------------------------------------------------------------- print views
E = lambda v: html.escape("" if v is None else str(v))


def money(v):
    v = D(v)
    s = f"{v:.2f}"
    whole, frac = s.split(".")
    neg = whole.startswith("-")
    whole = whole.lstrip("-")
    if len(whole) > 3:
        head, tail = whole[:-3], whole[-3:]
        parts = []
        while len(head) > 2:
            parts.insert(0, head[-2:])
            head = head[:-2]
        if head:
            parts.insert(0, head)
        whole = ",".join(parts) + "," + tail
    return ("-" if neg else "") + whole + "." + frac


def num(v):
    v = D(v)
    return f"{v.normalize():f}" if v == v.to_integral() else f"{v:.3f}".rstrip("0").rstrip(".")


PRINT_CSS = """<style>
*{box-sizing:border-box}body{font-family:Arial,Helvetica,sans-serif;color:#1d1d1f;margin:0;font-size:12px}
.page{max-width:820px;margin:20px auto;padding:28px;background:#fff}
.hd{display:flex;justify-content:space-between;align-items:center;border-bottom:3px solid #4C6F87;padding-bottom:12px}
.hd img{height:54px}.hd .co{text-align:right;font-size:11px;line-height:1.5;color:#444}
h1{font-size:18px;color:#15181B;margin:16px 0 4px;letter-spacing:.5px}
.meta{display:grid;grid-template-columns:1fr 1fr;gap:12px;margin:12px 0}.box{border:1px solid #d5dae3;padding:10px;border-radius:4px}
.box b{color:#4C6F87}table{width:100%;border-collapse:collapse;margin-top:8px}
th{background:#4C6F87;color:#fff;padding:7px;font-size:11px;text-align:left}td{border-bottom:1px solid #e3e6ec;padding:6px 7px}
.r{text-align:right}.tot td{border:none;padding:4px 7px}.tot .g td{font-weight:bold;font-size:14px;color:#4C6F87;border-top:2px solid #4C6F87}
.terms{white-space:pre-line;font-size:11px;color:#444;margin-top:16px}.sig{display:flex;justify-content:space-between;margin-top:50px}
.bar{height:4px;background:#A5A7A9;margin-top:24px}
@media print{.noprint{display:none}.page{margin:0;padding:10px}}
.noprint{text-align:center;margin:14px}.noprint button{background:#4C6F87;color:#fff;border:0;padding:9px 22px;border-radius:4px;cursor:pointer}
</style>"""


def print_header(s, title):
    return (f"<div class='hd'><img src='/logo.png' alt='Crayon'><div class='co'><b>{E(s.get('company_name'))}</b><br>"
            f"{E(s.get('company_address'))}<br>{E(s.get('company_phone'))} &middot; {E(s.get('company_email'))}"
            f"{'<br>GSTIN: ' + E(s.get('company_gstin')) if s.get('company_gstin') else ''}</div></div><h1>{title}</h1>")


@app.get("/print/quote/{rid}", response_class=HTMLResponse)
def print_quote(rid: int, u=Depends(user), c=Depends(db)):
    q = quote(rid, u, c)
    cu = c.execute("SELECT * FROM customers WHERE id=%s", (q["customer_id"],)).fetchone()
    s = settings(c)
    rows = "".join(
        f"<tr><td>{i + 1}</td><td>{E(it['description'])}{' - ' + E(it['colour']) if it['colour'] else ''}</td>"
        f"<td class='r'>{num(it['length_ft']) if D(it['length_ft']) else ''}</td><td class='r'>{num(it['pcs'])}</td>"
        f"<td class='r'>{num(it['qty'])} {E(it['uom'])}</td><td class='r'>{money(it['rate'])}</td>"
        f"<td class='r'>{num(it['gst_pct'])}%</td><td class='r'>{money(it['amount'])}</td></tr>"
        for i, it in enumerate(q["items"]))
    no = q["quote_no"] + (f" R{q['rev']}" if q["rev"] else "")
    disc = f"<tr><td class='r'>Discount ({num(q['discount_pct'])}%)</td><td class='r'>- {money(q['discount_amt'])}</td></tr>" if D(q["discount_amt"]) else ""
    frt = f"<tr><td class='r'>Freight</td><td class='r'>{money(q['freight'])}</td></tr>" if D(q["freight"]) else ""
    body = f"""<!doctype html><html><head><meta charset='utf-8'><title>{E(no)}</title>{PRINT_CSS}</head><body>
<div class='noprint'><button onclick='print()'>Print / Save as PDF</button></div><div class='page'>{print_header(s, 'QUOTATION')}
<div class='meta'><div class='box'><b>To</b><br>{E(cu['name'])}<br>{E(cu['address'])}<br>{E(cu['city'])}
{('<br>GSTIN: ' + E(cu['gstin'])) if cu['gstin'] else ''}{('<br>Ph: ' + E(cu['mobile'])) if cu['mobile'] else ''}</div>
<div class='box'><b>Quote No:</b> {E(no)}<br><b>Date:</b> {E(q['quote_date'])}<br><b>Valid till:</b> {E(q['valid_till'])}</div></div>
<table><tr><th>#</th><th>Description</th><th class='r'>Length (ft)</th><th class='r'>Pcs</th><th class='r'>Qty</th>
<th class='r'>Rate</th><th class='r'>GST</th><th class='r'>Amount</th></tr>{rows}</table>
<table class='tot' style='width:46%;margin-left:54%'><tr><td class='r'>Sub total</td><td class='r'>{money(q['subtotal'])}</td></tr>
{disc}<tr><td class='r'>GST</td><td class='r'>{money(q['gst_amt'])}</td></tr>{frt}
<tr class='g'><td class='r'>Total (Rs)</td><td class='r'>{money(q['total'])}</td></tr></table>
{('<p><b>Notes:</b> ' + E(q['notes']) + '</p>') if q['notes'] else ''}
<div class='terms'><b>Terms &amp; Conditions</b><br>{E(q['terms'])}</div>
<div class='sig'><div></div><div>For {E(s.get('company_name'))}<br><br><br>Authorised Signatory</div></div><div class='bar'></div></div></body></html>"""
    return body


@app.get("/print/dc/{rid}", response_class=HTMLResponse)
def print_dc(rid: int, u=Depends(user), c=Depends(db)):
    d = dispatch(rid, u, c)
    s = settings(c)
    rows = "".join(
        f"<tr><td>{i + 1}</td><td>{E(it['description'])}{' - ' + E(it['colour']) if it['colour'] else ''}</td>"
        f"<td class='r'>{num(it['length_ft']) if D(it['length_ft']) else ''}</td><td class='r'>{num(it['pcs'])}</td>"
        f"<td class='r'>{num(it['qty'])} {E(it['uom'])}</td></tr>" for i, it in enumerate(d["items"]))
    return f"""<!doctype html><html><head><meta charset='utf-8'><title>{E(d['dc_no'])}</title>{PRINT_CSS}</head><body>
<div class='noprint'><button onclick='print()'>Print / Save as PDF</button></div><div class='page'>{print_header(s, 'DELIVERY CHALLAN')}
<div class='meta'><div class='box'><b>Consignee</b><br>{E(d['customer'])}<br>{E(d['site_address'] or d['address'])}<br>{E(d['city'])}
{('<br>GSTIN: ' + E(d['gstin'])) if d['gstin'] else ''}</div>
<div class='box'><b>DC No:</b> {E(d['dc_no'])}<br><b>Date:</b> {E(d['dc_date'])}<br><b>SO No:</b> {E(d['so_no'])}
{('<br><b>Customer PO:</b> ' + E(d['customer_po'])) if d['customer_po'] else ''}<br><b>Vehicle:</b> {E(d['vehicle_no'])}
{('<br><b>Driver:</b> ' + E(d['driver_name']) + ' ' + E(d['driver_mobile'])) if d['driver_name'] else ''}
{('<br><b>Invoice:</b> ' + E(d['invoice_no'])) if d['invoice_no'] else ''}{('<br><b>E-way bill:</b> ' + E(d['eway_bill_no'])) if d['eway_bill_no'] else ''}</div></div>
<table><tr><th>#</th><th>Description</th><th class='r'>Length (ft)</th><th class='r'>Pcs</th><th class='r'>Qty</th></tr>{rows}</table>
<div class='sig'><div>Received in good condition<br><br><br>Customer signature</div><div>For {E(s.get('company_name'))}<br><br><br>Authorised Signatory</div></div>
<div class='bar'></div></div></body></html>"""


@app.get("/api/health")
def health():
    return {"ok": True, "build": BUILD}


@app.exception_handler(HTTPException)
async def http_err(request: Request, exc: HTTPException):
    if request.url.path.startswith("/print/") and exc.status_code == 401:
        return Response(status_code=302, headers={"Location": "/"})
    return JSONResponse({"detail": exc.detail}, status_code=exc.status_code)


# local dev: serve /public as static
if not os.environ.get("VERCEL"):
    from fastapi.staticfiles import StaticFiles
    pub = Path(__file__).resolve().parent.parent / "public"
    if pub.exists():
        app.mount("/", StaticFiles(directory=str(pub), html=True), name="static")

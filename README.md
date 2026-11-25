# Crayon Sales — Sales to Dispatch

The sales-to-dispatch app for Crayon Roofings & Structures, at **app.crayonroofings.com**.

**Flow:** Enquiry → Quotation → Sales Order (approval) → Cutting Program → Dispatch

**Stack:** FastAPI (Python) + Neon Postgres, hosted on Vercel. Build **C01**.

## Folder layout
```
api/index.py        backend (all API routes + quote/DC print pages)
public/index.html   frontend (single page)
public/logo.png     Crayon logo
requirements.txt    Python packages
vercel.json         routing for Vercel
.env.example        environment variables to set in Vercel
```

## Deploy
1. **Neon:** create a database called `crayon_sales` and copy its connection string.
2. **Vercel:** Add New → Project → import `BSC23609/crayon-sales`. Framework preset: **Other**.
3. **Environment variables** (Settings → Environment Variables):
   - `DATABASE_URL`: the Neon connection string
   - `SECRET_KEY`: any long random text
   - `ADMIN_EMAIL` / `ADMIN_PASSWORD`: the first admin login. This account is created automatically on the first visit.
4. Deploy, then open the site. Tables and sample products are created automatically.
5. **Domain:** Vercel → Settings → Domains → add `app.crayonroofings.com`, then add the CNAME record it shows (`app` → `cname.vercel-dns.com`) in the crayonroofings.com DNS.

## First-time setup in the app
1. **Settings:** enter the company address and GSTIN, the maximum discount without approval (default 3%) and the quotation terms.
2. **Products & Rates:** replace the sample items with the real rate list and set rates.
   - **Sell by:** running ft (`rft`), sq ft, kg, nos or box.
   - **Kg per rft:** needed only when a sheet is priced per kg.
   - **Cover width:** needed only when a sheet is priced per sq ft.
3. **Users:** add staff with roles `sales`, `approver`, `production`, `dispatch` or `admin`. Separate roles with commas.
4. **Customers:** enter a credit limit for each customer. A limit of 0 means cash or advance only.

## Rules built in
- **Numbering:** `CRS/ENQ|QT|SO|DC/26-27/0001`, resetting each financial year.
- **Quotes:** quantity = length × pieces for rft, kg and sq ft items. GST is applied line by line after the discount, and freight is added at the end. A revision creates R1, R2 and so on, and marks the old version as Revised.
- **Approval:** a sales order goes to **Pending Approval** when the discount is above the limit, or when the customer's open order balance minus the advance is above their credit limit. Otherwise it is approved automatically.
- **Production status:** planning cutting moves the order to *In Production*. When every cutting line is Done, the order becomes *Ready*.
- **Dispatch:** partial dispatches are allowed, and the app won't let you load more pieces than are left on the order. When everything has gone out, the order becomes *Dispatched*. The DC prints on Crayon letterhead.
- **Printing:** quotes and DCs print from the browser (Print → Save as PDF).

## Next phases
- WATI WhatsApp: send the quote or DC to the customer and use OTP login.
- Push invoices to Tally.
- Generate e-way bills through a GST provider. Until then, the e-way bill number is typed in by hand.
- Add a tile on the group app hub.

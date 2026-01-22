from flask import Flask, render_template, request, redirect, url_for, flash
import pandas as pd
import psycopg2
import os
import numpy as np

app = Flask(__name__)
app.secret_key = os.environ.get("SECRET_KEY", "supersecretkey123")

# =========================
# DB CONNECTION (Postgres)
# =========================
def get_db_connection():
    DATABASE_URL = os.environ.get('DATABASE_URL')
    if DATABASE_URL:
        url = DATABASE_URL.replace("postgres://", "postgresql://", 1)
        return psycopg2.connect(url, sslmode='require')
    else:
        # Local fallback
        return psycopg2.connect(
            host='c7s7ncbk19n97r.cluster-czrs8kj4isg7.us-east-1.rds.amazonaws.com',
            user='u7tqojjihbpn7s',
            password='p1b1897f6356bab4e52b727ee100290a84e4bf71d02e064e90c2c705bfd26f4a5',
            dbname='d8lp4hr6fmvb9m',
            port=5432,
            sslmode='require'
        )

# =========================
# SMART COLUMN CLEANING
# =========================
def clean_columns(df):
    df.columns = (
        df.columns.str.strip()
        .str.upper()
        .str.replace(" ", "_")
        .str.replace(r"[^\w\s]", "", regex=True)
    )

    mapping = {
        'VEHICLE': 'VEHICLE_NO',
        'VEHICLE_NUMBER': 'VEHICLE_NO',
        'INDENT': 'INDENT_ID',
        'INDENT_NO': 'INDENT_ID',
        'DATE': 'TRIP_DATE',
        'FROM': 'FROM_LOCATION',
        'TO': 'TO_LOCATION',
        'CHARGING': 'CHARGING_COST',
        'TOTAL_COST': 'TOTAL_TRIP_COST'
    }
    return df.rename(columns=mapping)

# =========================
# WELCOME PAGE (Add kiya gaya hai)
# =========================
@app.route("/")
def welcome():
    return render_template("welcome.html")

# =========================
# UPLOAD LOGIC
# =========================

def clean_columns(df):
    # simple cleaning: strip spaces and uppercase column names
    df.columns = df.columns.str.strip().str.upper()
    return df

@app.route("/upload", methods=["GET", "POST"])
def upload_file():
    if request.method == "POST":
        vehicle_file = request.files.get("file")
        cash_file = request.files.get("cash_file")

        if not vehicle_file:
            flash("Vehicle expense file required", "warning")
            return redirect(request.url)

        try:
            # =====================================
            # VEHICLE EXPENSE FILE
            # =====================================
            if vehicle_file.filename.endswith(".csv"):
                df = pd.read_csv(vehicle_file)
            else:
                df = pd.read_excel(vehicle_file)

            # Clean column names
            df.columns = (
                df.columns.str.strip()
                .str.upper()
                .str.replace(" ", "_")
            )

            # Rename Excel → DB columns (UNCHANGED)
            df.rename(columns={
                "VEHICLE_NO": "VEHICLE_NO",
                "TOTAL_RUNING_KM": "TOTAL_RUNING_KM",
                "NO_OF_BUKEET": "NO_OF_BUKEET",
                "CNG_RATE": "CNG_RATE",
                "USED_CNG": "USED_CNG",
                "PAY_TO_CNG": "PAY_TO_CNG",
                "UNLOADING_CHARGE": "UNLOADING_CHARGE",
                "TOTAL_COST": "TOTAL_COST",
                "OTHER_EXP": "OTHER_EXP",        # ✅ ADD
                "ON_ACCOUNT": "ON_ACCOUNT"       # ✅ ADD
            }, inplace=True)

            # Required columns (including ON_ACCOUNT)
            required_cols = [
                "VEHICLE_NO", "DATE", "FROM", "TO",
                "TOTAL_RUNING_KM", "NO_OF_BUKEET",
                "CNG_RATE", "USED_CNG", "PAY_TO_CNG",
                "AVERAGE", "TOLL", "UNLOADING_CHARGE",
                "OTHER_EXP",                      # ✅ ADD
                "REMARK", "ADVANCE", "TOTAL_COST",
                "ON_ACCOUNT"                      # ✅ ADD
            ]

            for col in required_cols:
                if col not in df.columns:
                    df[col] = 0

            df["DATE"] = pd.to_datetime(df["DATE"], errors="coerce")

            # Numeric columns (including ON_ACCOUNT)
            num_cols = [
                "TOTAL_RUNING_KM", "NO_OF_BUKEET",
                "CNG_RATE", "USED_CNG", "PAY_TO_CNG",
                "AVERAGE", "TOLL",
                "UNLOADING_CHARGE",
                "OTHER_EXP",                      # ✅ ADD
                "ADVANCE", "TOTAL_COST",
                "ON_ACCOUNT"                      # ✅ ADD
            ]

            for col in num_cols:
                df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

            conn = get_db_connection()
            cur = conn.cursor()

            for _, r in df.iterrows():
                cur.execute("""
                    INSERT INTO vehicle_expenses (
                        vehicle_no, date, "FROM", "TO",
                        total_runing_km, no_of_bukeet,
                        cng_rate, used_cng, pay_to_cng,
                        average, toll, unloading_charge,
                        other_exp,                      -- ✅ ADD
                        remark, advance, total_cost,
                        on_account                       -- ✅ ADD
                    ) VALUES (
                        %s,%s,%s,%s,
                        %s,%s,%s,%s,%s,
                        %s,%s,%s,%s,%s,%s,%s,%s
                    )
                """, (
                    r["VEHICLE_NO"],
                    r["DATE"].date() if pd.notna(r["DATE"]) else None,
                    r["FROM"],
                    r["TO"],
                    r["TOTAL_RUNING_KM"],
                    r["NO_OF_BUKEET"],
                    r["CNG_RATE"],
                    r["USED_CNG"],
                    r["PAY_TO_CNG"],
                    r["AVERAGE"],
                    r["TOLL"],
                    r["UNLOADING_CHARGE"],
                    r["OTHER_EXP"],                 # ✅ ADD
                    r["REMARK"],
                    r["ADVANCE"],
                    r["TOTAL_COST"],
                    r["ON_ACCOUNT"]                 # ✅ ADD
                ))

            # =====================================
            # CASH EXPENSE FILE (UNCHANGED)
            # =====================================
            if cash_file:
                if cash_file.filename.endswith(".csv"):
                    df_cash = pd.read_csv(cash_file)
                else:
                    df_cash = pd.read_excel(cash_file)

                df_cash.columns = (
                    df_cash.columns.str.strip()
                    .str.upper()
                    .str.replace(" ", "_")
                )

                if "CASH_RECEIVED" not in df_cash.columns:
                    df_cash["CASH_RECEIVED"] = 0

                if "CASH_RECEIVED_DATE" not in df_cash.columns:
                    df_cash["CASH_RECEIVED_DATE"] = None

                df_cash["CASH_RECEIVED"] = pd.to_numeric(
                    df_cash["CASH_RECEIVED"], errors="coerce"
                ).fillna(0)

                df_cash["CASH_RECEIVED_DATE"] = pd.to_datetime(
                    df_cash["CASH_RECEIVED_DATE"], errors="coerce"
                )

                for _, r in df_cash.iterrows():
                    cur.execute("""
                        INSERT INTO cash_expenses (
                            cash_received, cash_received_date
                        ) VALUES (%s,%s)
                    """, (
                        r["CASH_RECEIVED"],
                        r["CASH_RECEIVED_DATE"].date()
                        if pd.notna(r["CASH_RECEIVED_DATE"]) else None
                    ))

            conn.commit()
            cur.close()
            conn.close()

            flash("Vehicle + Cash data uploaded successfully ✅", "success")
            return redirect(url_for("upload_file"))

        except Exception as e:
            print("UPLOAD ERROR:", e)
            flash(str(e), "danger")
            return redirect(request.url)

    return render_template("upload.html")




# =========================
# DASHBOARD
# =========================



@app.route("/dashboard")
def dashboard():
    conn = get_db_connection()
    cur = conn.cursor()

    # -------------------------------
    # 1️⃣ All Vehicle Expense Records
    # -------------------------------
    cur.execute("""
        SELECT *
        FROM vehicle_expenses
        ORDER BY DATE DESC NULLS LAST
    """)
    columns = [desc[0] for desc in cur.description]
    rows_raw = cur.fetchall()

    rows = []
    for r in rows_raw:
        row_dict = dict(zip(columns, r))
        if row_dict.get("DATE"):
            row_dict["DATE"] = row_dict["DATE"].strftime("%d-%m-%y")
        rows.append(row_dict)

    # -------------------------------
    # 2️⃣ Amount Received (CARD FIXED)
    # -------------------------------
    cur.execute("""
        SELECT COALESCE(SUM(cash_received),0)
        FROM cash_expenses
    """)
    total_cash_received = float(cur.fetchone()[0] or 0)

    # -------------------------------
    # 3️⃣ Vehicle-wise Summary
    # -------------------------------
    cur.execute("""
        SELECT
            VEHICLE_NO,
            COALESCE(SUM(ADVANCE),0)           AS driver_advance,
            COALESCE(SUM(TOTAL_COST),0)        AS total_trip_cost,
            COALESCE(SUM(UNLOADING_CHARGE),0)  AS unloading_charge,
            COALESCE(SUM(total_runing_km),0)   AS total_km,
            COALESCE(AVG(AVERAGE),0)           AS avg_mileage,
            COALESCE(AVG(CNG_RATE),0)          AS cng_rate,
            COALESCE(SUM(ON_ACCOUNT),0)        AS on_account
        FROM vehicle_expenses
        GROUP BY VEHICLE_NO
        ORDER BY VEHICLE_NO
    """)
    summary_rows = cur.fetchall()

    summary = []
    remaining_amount = total_cash_received   # 🔑 Wallet logic unchanged
    EV_COST = 6980                           # 🔑 Fixed EV cost

    for r in summary_rows:
        vehicle = r[0]
        driver_advance = float(r[1])
        total_trip_cost = float(r[2])
        unloading_charge = float(r[3])
        total_km = float(r[4])
        avg_mileage = float(r[5])
        cng_rate = float(r[6])
        on_account = float(r[7])

        # -------------------------------
        # Revenue Calculation (UNCHANGED)
        # -------------------------------
        fuel_cost = 0
        if avg_mileage > 0:
            fuel_cost = ((total_km + 20) / avg_mileage) * cng_rate

        revenue = unloading_charge - fuel_cost

        # -------------------------------
        # Wallet Deduction (UNCHANGED)
        # -------------------------------
        vehicle_balance = remaining_amount - total_trip_cost - on_account
        pending_balance = vehicle_balance if vehicle_balance > 0 else 0
        remaining_amount = vehicle_balance

        # -------------------------------
        # ✅ EV COST ADDED ONLY TO TOTAL COST (DISPLAY)
        # -------------------------------
        display_total_cost = total_trip_cost + EV_COST

        summary.append({
            "vehicle": vehicle,
            "driver_advance": driver_advance,
            "total_trip_cost": total_trip_cost,        # DB cost (actual)
            "amount_received": total_cash_received,   # CARD fixed
            "total_cost": display_total_cost,          # ✅ EV cost added
            "revenue": round(revenue, 2),
            "on_account": on_account,
            "balance": vehicle_balance,
            "ev_cost": EV_COST,                        # Optional display
            "pending_balance": pending_balance
        })

    cur.close()
    conn.close()

    return render_template(
        "dashboard.html",
        rows=rows,
        columns=columns,
        summary=summary,
        amount_received=total_cash_received   # CARD value only
    )




if __name__ == "__main__":
    # Heroku ke liye port configuration
    port = int(os.environ.get("PORT", 5000))
    app.run(host='0.0.0.0', port=port)
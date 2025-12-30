from flask import Flask, render_template, request, redirect, url_for, flash
import pandas as pd
import psycopg2
import os
import numpy as np

app = Flask(__name__)

# =========================
# SECRET KEY FOR FLASH
# =========================
app.secret_key = "supersecretkey123"  # ⚡ Add your own unique secret key

UPLOAD_FOLDER = "uploads"
os.makedirs(UPLOAD_FOLDER, exist_ok=True)

# =========================
# POSTGRES CONFIG
# =========================
db_config = {
    'host': 'c7s7ncbk19n97r.cluster-czrs8kj4isg7.us-east-1.rds.amazonaws.com',
    'user': 'u7tqojjihbpn7s',
    'password': 'p1b1897f6356bab4e52b727ee100290a84e4bf71d02e064e90c2c705bfd26f4a5',
    'database': 'd8lp4hr6fmvb9m',
    'port': 5432
}

def get_db_connection():
    return psycopg2.connect(
        host=db_config['host'],
        user=db_config['user'],
        password=db_config['password'],
        dbname=db_config['database'],
        port=db_config['port']
    )

# =========================
# COLUMN CLEANING
# =========================
def clean_columns(df):
    df.columns = (
        df.columns.str.strip()
        .str.upper()
        .str.replace(" ", "_")
        .str.replace(".", "", regex=False)
    )
    return df

# =========================
# WELCOME PAGE (ROOT)
# =========================
@app.route("/")
def welcome():
    return render_template("welcome.html")

# =========================
# UPLOAD PAGE
# =========================
@app.route("/upload", methods=["GET", "POST"])
def upload_file():
    if request.method == "POST":
        file = request.files.get("file")
        if not file or not file.filename.endswith((".xlsx", ".csv")):
            flash("Invalid file format! Only CSV or Excel allowed.", "danger")
            return redirect(request.url)

        path = os.path.join(UPLOAD_FOLDER, file.filename)
        file.save(path)

        # Read file
        df = pd.read_csv(path) if file.filename.endswith(".csv") else pd.read_excel(path)
        df = clean_columns(df)

        required_cols = [
            "VEHICLE_NO", "INDENT_ID", "DATE", "FROM", "TO",
            "CHARGING_COST", "TOLL", "DRIVER_ON_ACCOUNT",
            "OTHER_EXP", "REMARK", "TOTAL_TRIP_COST"
        ]

        missing = set(required_cols) - set(df.columns)
        if missing:
            flash(
                "Columns mismatch! ❌ Please download the template and upload data in the correct format.",
                "danger"
            )
            return redirect(request.url)

        # =========================
        # DATA CLEANING
        # =========================
        df["DATE"] = pd.to_datetime(df["DATE"], errors="coerce")
        df = df.replace({np.nan: None})

        numeric_cols = [
            "CHARGING_COST", "TOLL",
            "DRIVER_ON_ACCOUNT", "OTHER_EXP",
            "TOTAL_TRIP_COST"
        ]
        for col in numeric_cols:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

        text_cols = ["VEHICLE_NO", "INDENT_ID", "FROM", "TO", "REMARK"]
        for col in text_cols:
            df[col] = df[col].astype(str).replace("None", "")

        # =========================
        # DB INSERT
        # =========================
        conn = get_db_connection()
        cur = conn.cursor()
        for _, r in df.iterrows():
            cur.execute("""
                INSERT INTO vehicle_expenses (
                    vehicle_no, indent_id, trip_date,
                    from_location, to_location,
                    charging_cost, toll,
                    driver_on_account, other_exp,
                    remark, total_trip_cost
                ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """, (
                r["VEHICLE_NO"], r["INDENT_ID"],
                r["DATE"].date() if r["DATE"] else None,
                r["FROM"], r["TO"],
                r["CHARGING_COST"], r["TOLL"],
                r["DRIVER_ON_ACCOUNT"], r["OTHER_EXP"],
                r["REMARK"], r["TOTAL_TRIP_COST"]
            ))
        conn.commit()
        cur.close()
        conn.close()

        flash("File uploaded successfully! ✅", "success")
        return redirect(url_for("upload_file"))

    return render_template("upload.html")


# =========================
# DASHBOARD
# =========================
@app.route("/dashboard")
def dashboard():
    conn = get_db_connection()
    cur = conn.cursor()

    cur.execute("SELECT * FROM vehicle_expenses ORDER BY trip_date DESC")
    rows = cur.fetchall()

    cur.execute("""
        SELECT vehicle_no, SUM(total_trip_cost)
        FROM vehicle_expenses
        GROUP BY vehicle_no
    """)
    chart = cur.fetchall()

    cur.close()
    conn.close()

    return render_template(
        "dashboard.html",
        rows=rows,
        vehicles=[c[0] for c in chart],
        totals=[float(c[1]) for c in chart]
    )


if __name__ == "__main__":
    app.run(debug=True)

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
@app.route("/upload", methods=["GET", "POST"])
def upload_file():
    if request.method == "POST":
        file = request.files.get("file")
        if not file:
            flash("Please select a file.", "warning")
            return redirect(request.url)

        try:
            if file.filename.endswith(".csv"):
                df = pd.read_csv(file)
            else:
                df = pd.read_excel(file)

            df = clean_columns(df)

            db_cols = [
                "VEHICLE_NO", "INDENT_ID", "TRIP_DATE", "FROM_LOCATION",
                "TO_LOCATION", "CHARGING_COST", "TOLL", "DRIVER_ON_ACCOUNT",
                "OTHER_EXP", "REMARK", "TOTAL_TRIP_COST"
            ]
            for col in db_cols:
                if col not in df.columns:
                    df[col] = None

            df["TRIP_DATE"] = pd.to_datetime(df["TRIP_DATE"], errors="coerce")

            num_cols = ["CHARGING_COST", "TOLL", "DRIVER_ON_ACCOUNT", "OTHER_EXP", "TOTAL_TRIP_COST"]
            for col in num_cols:
                df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

            df = df.replace({np.nan: None})

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
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    str(r["VEHICLE_NO"]) if r["VEHICLE_NO"] else "",
                    str(r["INDENT_ID"]) if r["INDENT_ID"] else "",
                    r["TRIP_DATE"].date() if r["TRIP_DATE"] else None,
                    str(r["FROM_LOCATION"]) if r["FROM_LOCATION"] else "",
                    str(r["TO_LOCATION"]) if r["TO_LOCATION"] else "",
                    float(r["CHARGING_COST"]),
                    float(r["TOLL"]),
                    float(r["DRIVER_ON_ACCOUNT"]),
                    float(r["OTHER_EXP"]),
                    str(r["REMARK"]) if r["REMARK"] else "",
                    float(r["TOTAL_TRIP_COST"])
                ))

            conn.commit()
            cur.close()
            conn.close()

            flash("Data Uploaded Successfully! ✅", "success")
            return redirect(url_for("upload_file"))

        except Exception as e:
            flash(f"Error: {str(e)}", "danger")
            return redirect(request.url)

    return render_template("upload.html")

# =========================
# DASHBOARD
# =========================
@app.route("/dashboard")
def dashboard():
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("SELECT * FROM vehicle_expenses ORDER BY trip_date DESC NULLS LAST")
    rows = cur.fetchall()

    cur.execute("SELECT vehicle_no, SUM(total_trip_cost) FROM vehicle_expenses GROUP BY vehicle_no")
    chart_data = cur.fetchall()

    cur.close()
    conn.close()

    return render_template(
        "dashboard.html",
        rows=rows,
        vehicles=[c[0] for c in chart_data],
        totals=[float(c[1]) for c in chart_data]
    )

if __name__ == "__main__":
    # Heroku ke liye port configuration
    port = int(os.environ.get("PORT", 5000))
    app.run(host='0.0.0.0', port=port)
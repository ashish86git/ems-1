from flask import Flask, render_template, request, redirect, url_for, flash
import pandas as pd
import psycopg2
import os
import numpy as np
import io

app = Flask(__name__)

# =========================
# CONFIGURATION
# =========================
app.secret_key = os.environ.get("SECRET_KEY", "supersecretkey123")
DATABASE_URL = os.environ.get('DATABASE_URL')


def get_db_connection():
    if DATABASE_URL:
        # Heroku connection logic
        url = DATABASE_URL.replace("postgres://", "postgresql://", 1)
        return psycopg2.connect(url, sslmode='require')
    else:
        # Local/RDS testing logic
        return psycopg2.connect(
            host='c7s7ncbk19n97r.cluster-czrs8kj4isg7.us-east-1.rds.amazonaws.com',
            user='u7tqojjihbpn7s',
            password='p1b1897f6356bab4e52b727ee100290a84e4bf71d02e064e90c2c705bfd26f4a5',
            dbname='d8lp4hr6fmvb9m',
            port=5432,
            sslmode='require'
        )


# =========================
# COLUMN CLEANING (Exact Matching)
# =========================
def clean_columns(df):
    # Sabhi headers ko uppercase aur underscores mein badalna
    df.columns = (
        df.columns.str.strip()
        .str.upper()
        .str.replace(" ", "_")
        .str.replace(r"[^\w\s]", "", regex=True)
    )

    mapping = {
        # FROM Mapping
        'FROM_LOCATION': 'FROM',
        'SOURCE': 'FROM',
        'START_POINT': 'FROM',
        'PICKUP': 'FROM',
        'ORIGIN': 'FROM',

        # TO Mapping
        'TO_LOCATION': 'TO',
        'DESTINATION': 'TO',
        'END_POINT': 'TO',
        'DROPOFF': 'TO',

        # Baki columns (For safety)
        'VEHICLE': 'VEHICLE_NO',
        'INDENT': 'INDENT_ID',
        'TRIP_DATE': 'DATE',
        'TOTAL_COST': 'TOTAL_TRIP_COST'
    }

    df = df.rename(columns=mapping)
    return df

# =========================
# ROUTES
# =========================

@app.route("/")
def welcome():
    return render_template("welcome.html")


@app.route("/upload", methods=["GET", "POST"])
def upload_file():
    if request.method == "POST":
        file = request.files.get("file")
        if not file or not file.filename.endswith((".xlsx", ".csv")):
            flash("Invalid file format! Only CSV or Excel allowed.", "danger")
            return redirect(request.url)

        try:
            # Heroku par file save karne ki jagah direct memory (stream) se read karna best hai
            if file.filename.endswith(".csv"):
                df = pd.read_csv(file)
            else:
                df = pd.read_excel(file)

            df = clean_columns(df)

            # Matching with the keys used in INSERT logic
            required_cols = [
                "VEHICLE_NO", "INDENT_ID", "DATE", "FROM", "TO",
                "CHARGING_COST", "TOLL", "DRIVER_ON_ACCOUNT",
                "OTHER_EXP", "REMARK", "TOTAL_TRIP_COST"
            ]

            missing = set(required_cols) - set(df.columns)
            if missing:
                flash(f"Missing columns: {', '.join(missing)} ❌", "danger")
                return redirect(request.url)

            # =========================
            # DATA CLEANING
            # =========================
            df["DATE"] = pd.to_datetime(df["DATE"], errors="coerce")

            numeric_cols = ["CHARGING_COST", "TOLL", "DRIVER_ON_ACCOUNT", "OTHER_EXP", "TOTAL_TRIP_COST"]
            for col in numeric_cols:
                df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

            df = df.replace({np.nan: None})

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
                    str(r["VEHICLE_NO"]), str(r["INDENT_ID"]),
                    r["DATE"].date() if r["DATE"] else None,
                    str(r["FROM"]), str(r["TO"]),
                    float(r["CHARGING_COST"]), float(r["TOLL"]),
                    float(r["DRIVER_ON_ACCOUNT"]), float(r["OTHER_EXP"]),
                    str(r["REMARK"]) if r["REMARK"] else "",
                    float(r["TOTAL_TRIP_COST"])
                ))
            conn.commit()
            cur.close()
            conn.close()

            flash("File uploaded successfully! ✅", "success")
            return redirect(url_for("upload_file"))

        except Exception as e:
            flash(f"System Error: {str(e)}", "danger")
            return redirect(request.url)

    return render_template("upload.html")


@app.route("/dashboard")
def dashboard():
    try:
        conn = get_db_connection()
        cur = conn.cursor()

        # Dashboard data
        cur.execute("SELECT * FROM vehicle_expenses ORDER BY trip_date DESC")
        rows = cur.fetchall()

        # Chart data
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
    except Exception as e:
        return f"Dashboard Error: {str(e)}"


if __name__ == "__main__":
    app.run(debug=True)
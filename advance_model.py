import joblib
import pandas as pd

MODEL = joblib.load("advance_predictor.pkl")

def predict_min_advance(
    total_km,
    cng_rate,
    unloading_charge,
    average
):
    fuel_cost = (total_km / average) * cng_rate if average > 0 else 0

    df = pd.DataFrame([{
        "total_runing_km": total_km,
        "cng_rate": cng_rate,
        "unloading_charge": unloading_charge,
        "average": average,
        "fuel_cost": fuel_cost
    }])

    advance = MODEL.predict(df)[0]

    # 🔐 Safety buffer (real world)
    return round(max(advance, fuel_cost * 1.05), 0)

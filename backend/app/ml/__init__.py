from app.ml.dummy_model import DummyRiskModel, FEATURES, LEVEL_BREAKS, risk_level 

_model = DummyRiskModel()

def get_model():
    return _model

def build_feature_row(cell, daily):
    return {
        "fwi": daily.fwi,
        "ndvi": daily.ndvi,
        "rainfall_mm": daily.rainfall_mm,
    }
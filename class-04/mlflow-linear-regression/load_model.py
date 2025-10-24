# load_model.py
import sys
import mlflow
import numpy as np

# replace with your actual run_id from the UI
run_id = sys.argv[1] if len(sys.argv) > 1 else "<RUN_ID_FROM_UI>"
model_uri = f"runs:/{run_id}/model"
model = mlflow.pyfunc.load_model(model_uri)
example = np.random.randn(1, 10)  # Diabetes has 10
print("Prediction:", model.predict(example))

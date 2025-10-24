import os
import argparse
import tempfile
import numpy as np
import matplotlib.pyplot as plt
import mlflow
import mlflow.sklearn
from sklearn.datasets import load_diabetes
from sklearn.linear_model import Ridge
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_squared_error, mean_absolute_error, r2_score


def main(alpha: float, test_size: float, random_state: int):
    # Optional: set a local tracking dir (comment out if using default ./mlruns)
    # mlflow.set_tracking_uri("file://" + os.path.abspath("mlruns"))
    mlflow.set_experiment("LinearModel-Diabetes")

    with mlflow.start_run():
        # 1) data
        data = load_diabetes()
        X, y = data.data, data.target
        X_train, X_test, y_train, y_test = train_test_split(
            X, y, test_size=test_size, random_state=random_state
        )

        # 2) model
        mlflow.sklearn.autolog(log_models=True)
        model = Ridge(alpha=alpha, random_state=random_state)
        model.fit(X_train, y_train)

        # 3) predictions + metrics
        y_pred = model.predict(X_test)
        rmse = mean_squared_error(y_test, y_pred, squared=False)
        mae = mean_absolute_error(y_test, y_pred)
        r2 = r2_score(y_test, y_pred)

        # 4) log params + metrics
        mlflow.log_param("alpha", alpha)
        mlflow.log_param("test_size", test_size)
        mlflow.log_param("random_state", random_state)

        mlflow.log_metric("rmse", rmse)
        mlflow.log_metric("mae", mae)
        mlflow.log_metric("r2", r2)

        # 5) residuals plot as an artifact

        resid = y_test - y_pred
        with tempfile.TemporaryDirectory() as td:
            plot_path = os.path.join(td, "residuals.png")
            plt.figure()
            plt.scatter(y_pred, resid, alpha=0.6)
            plt.axhline(0, linestyle="--")
            plt.xlabel("Predicted")
            plt.ylabel("Residual (y_true - y_pred)")
            plt.title("Residuals Plot (Ridge)")
            plt.savefig(plot_path, bbox_inches="tight")
            plt.close()
            mlflow.log_artifact(plot_path, artifact_path="plots")

        # 6) log model
        mlflow.sklearn.log_model(
            model,
            artifact_path="model",
            input_example=X_test[:5],
            registered_model_name=None,  # set a name here if you use a registry
        )

        print(f"Run complete. Metrics => RMSE: {rmse:.3f} MAE: {mae:.3f} R2: {r2:.3f}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--alpha", type=float, default=1.0)
    parser.add_argument("--test_size", type=float, default=0.2)
    parser.add_argument("--random_state", type=int, default=42)
    args = parser.parse_args()
    main(args.alpha, args.test_size, args.random_state)

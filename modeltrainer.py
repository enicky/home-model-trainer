import numpy as np
import pandas as pd
import torch
import torch.nn as nn
from sklearn.preprocessing import StandardScaler
from torch.utils.data import DataLoader

from model.nbeats import NBeats
from model.timeseriesdataset import TimeSeriesDataset


class ModelTrainer:
    def __init__(self):
        # CONFIG
        self.FORECAST_HORIZON = 7 * 24 * 4  # 7 days, 15 min intervals
        self.INPUT_SEQ_LEN = 7 * 24 * 4    # use past week to predict next week
        self.BATCH_SIZE = 32
        self.EPOCHS = 20
        self.LR = 0.001
        self.device = torch.device("mps" if torch.backends.mps.is_available() else "cpu")
        print(f"Using device: {self.device}")
        self.scaler = StandardScaler()
        self.model = None
        self.train_losses = []
        self.val_losses = []

    def load_and_preprocess_data(self):
        dates = pd.date_range("2024-01-01", periods=365*24*60, freq='min')
        df = pd.DataFrame({
            "timestamp": dates,
            "power": np.sin(np.arange(len(dates)) / 1440 * 2 * np.pi) + np.random.randn(len(dates)) * 0.1,
            "temp": 20 + 10 * np.sin(np.arange(len(dates)) / 10000 * 2 * np.pi),
            "humidity": 50 + 20 * np.cos(np.arange(len(dates)) / 5000 * 2 * np.pi)
        })
        df = df.set_index("timestamp").resample("15min").mean().reset_index()
        df["hour"] = df["timestamp"].dt.hour
        df["dayofweek"] = df["timestamp"].dt.dayofweek
        df["hour_sin"] = np.sin(2 * np.pi * df["hour"] / 24)
        df["hour_cos"] = np.cos(2 * np.pi * df["hour"] / 24)
        df["dow_sin"] = np.sin(2 * np.pi * df["dayofweek"] / 7)
        df["dow_cos"] = np.cos(2 * np.pi * df["dayofweek"] / 7)
        feature_cols = ["power", "temp", "humidity", "hour_sin", "hour_cos", "dow_sin", "dow_cos"]
        df_scaled = self.scaler.fit_transform(df[feature_cols])
        self.df = df
        self.df_scaled = df_scaled

    def setup_datasets(self):
        train_size = int(len(self.df_scaled) * 0.8)
        train_data = self.df_scaled[:train_size]
        val_data = self.df_scaled[train_size:]
        self.train_dataset = TimeSeriesDataset(train_data, self.INPUT_SEQ_LEN, self.FORECAST_HORIZON)
        self.val_dataset = TimeSeriesDataset(val_data, self.INPUT_SEQ_LEN, self.FORECAST_HORIZON)
        self.train_loader = DataLoader(self.train_dataset, batch_size=self.BATCH_SIZE, shuffle=True)
        self.val_loader = DataLoader(self.val_dataset, batch_size=1, shuffle=False)

    def setup_model(self):
        self.model = NBeats(input_len=self.INPUT_SEQ_LEN*7, forecast_len=self.FORECAST_HORIZON).to(self.device)
        self.optimizer = torch.optim.Adam(self.model.parameters(), lr=self.LR)
        self.criterion = nn.MSELoss()

    def train(self):
        for epoch in range(self.EPOCHS):
            self.model.train()
            epoch_train_loss = 0
            for x, y in self.train_loader:
                x, y = x.to(self.device), y.to(self.device)
                self.optimizer.zero_grad()
                pred = self.model(x.view(x.size(0), -1))
                loss = self.criterion(pred, y)
                loss.backward()
                self.optimizer.step()
                epoch_train_loss += loss.item()
            avg_train_loss = epoch_train_loss / len(self.train_loader)
            self.train_losses.append(avg_train_loss)
            self.model.eval()
            epoch_val_loss = 0
            with torch.no_grad():
                for x_val, y_val in self.val_loader:
                    x_val, y_val = x_val.to(self.device), y_val.to(self.device)
                    pred_val = self.model(x_val.view(x_val.size(0), -1))
                    val_loss = self.criterion(pred_val, y_val)
                    epoch_val_loss += val_loss.item()
            avg_val_loss = epoch_val_loss / len(self.val_loader)
            self.val_losses.append(avg_val_loss)
            print(f"Epoch {epoch + 1}/{self.EPOCHS}: Train Loss = {avg_train_loss:.4f}, Validation Loss = {avg_val_loss:.4f}")

    def forecast(self):
        self.model.eval()
        with torch.no_grad():
            input_data = torch.tensor(self.df_scaled[-self.INPUT_SEQ_LEN:], dtype=torch.float32).to(self.device).unsqueeze(0)
            forecast = self.model(input_data.reshape(input_data.size(0), -1))
            forecast_scaled = forecast.cpu().numpy().flatten()
            zeros = np.zeros_like(forecast_scaled)
            stacked = np.column_stack([forecast_scaled, zeros, zeros, zeros, zeros, zeros, zeros])
            forecast_unscaled = self.scaler.inverse_transform(stacked)[:, 0]
        return forecast_unscaled

    def plot_forecast(self, forecast_unscaled):
        import matplotlib.pyplot as plt
        df = self.df
        plt.figure(figsize=(10, 6))
        plt.plot(df['timestamp'][-self.FORECAST_HORIZON:], forecast_unscaled, label='Forecast', color='red')
        plt.plot(df['timestamp'][-self.FORECAST_HORIZON:], df['power'][-self.FORECAST_HORIZON:], label='Actual', color='blue')
        plt.xlabel('Time')
        plt.ylabel('Power Consumption')
        plt.title('7-day Power Consumption Forecast')
        plt.legend()
        plt.show()

    def train_and_forecast(self):
        self.load_and_preprocess_data()
        self.setup_datasets()
        self.setup_model()
        self.train()
        forecast_unscaled = self.forecast()
        self.plot_forecast(forecast_unscaled)
        print("N-BEATS forecast complete!")

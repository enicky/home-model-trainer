import torch
import torch.nn as nn


class NBeatsBlock(nn.Module):
    def __init__(self, input_dim, hidden_dim, theta_dim, output_dim, n_layers=4):
        super().__init__()
        layers = []
        for _ in range(n_layers):
            layers.append(nn.Linear(input_dim if len(layers)==0 else hidden_dim, hidden_dim))
            layers.append(nn.ReLU())
        layers.append(nn.Linear(hidden_dim, theta_dim))
        self.fc = nn.Sequential(*layers)
        self.backcast_lin = nn.Linear(theta_dim, input_dim)
        self.forecast_lin = nn.Linear(theta_dim, output_dim)

    def forward(self, x):
        theta = self.fc(x)
        backcast = self.backcast_lin(theta)
        forecast = self.forecast_lin(theta)
        return backcast, forecast

from torch import nn
from model.nbeatsblok import NBeatsBlock


class NBeats(nn.Module):
    def __init__(self, input_len, forecast_len, hidden_dim=256, nb_blocks=6, n_layers=4, theta_dim=512):
        super().__init__()
        self.blocks = nn.ModuleList([
            NBeatsBlock(input_len, hidden_dim, theta_dim, forecast_len, n_layers=n_layers)
            for _ in range(nb_blocks)
        ])

    def forward(self, x):
        residual = x.view(x.size(0), -1)
        forecast = 0
        for block in self.blocks:
            backcast, block_forecast = block(residual)
            residual = residual - backcast
            forecast = forecast + block_forecast
        return forecast

FORECAST_HORIZON = 7 * 24 * 4  # 7 days, 15 min intervals
INPUT_SEQ_LEN = 7 * 24 * 4    # use past week to predict next week
BATCH_SIZE = 32
EPOCHS = 20
LR = 0.001
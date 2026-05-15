import pandas as pd

for file in ["chronic.csv", "heart.csv", "nutri.csv"]:
    df = pd.read_csv(file, nrows=100)
    df.to_csv(f"chunk_{file}", index=False)
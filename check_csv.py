import pandas as pd
df = pd.read_csv('./jobs/PlayerStatistics.csv', low_memory=False)
print('Total rows:', len(df))
print('Min date:', df['gameDateTimeEst'].min())
print('Max date:', df['gameDateTimeEst'].max())
print('Unique years:', df['gameDateTimeEst'].str[:4].nunique())
import pandas as pd
from matplotlib import pyplot as plt

df = pd.read_csv('full_game_log_nba.csv')
corrMatrix = df.corr(method='kendall')
print(corrMatrix)
# plt.matshow(corrMatrix)
# plt.show()
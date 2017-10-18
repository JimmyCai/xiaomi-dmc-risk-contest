import pandas as pd
import numpy as np
from minepy import MINE
import sys

if __name__ == "__main__":
    first_path, second_path = sys.argv[1:3]

    first_df = pd.read_csv(first_path)
    second_df = pd.read_csv(second_path)

    m = MINE()
    m.compute_score(first_df['ovd_rate'].values, second_df['ovd_rate'].values)
    print m.mic()


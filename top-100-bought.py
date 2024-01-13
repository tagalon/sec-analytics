mask_1 = all_holding_changes['DeltaRelative'] > 0
mask_2 = all_holding_changes['DeltaRelative'] != np.inf

bought_all = all_holding_changes[(mask_1 & mask_2)].sort_values(by=['DeltaRelative'], ascending=False)

top_100_bought = bought_all[bought_all['Shares2021_03_31'] > 500000][:100].reset_index()

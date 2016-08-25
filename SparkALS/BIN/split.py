#!/usr/bin/env python
import pandas as pd

import csv

# one based CSR
#

# prerequsite: sorted array for [val,row,col]
#
def get_offsets(rowIdx, startIdx, endIdx):
    rowIdx = rowIdx.sub(startIdx)
    value_cnt = rowIdx.value_counts()
    rowOffsets = [1]
    for row in xrange(0, endIdx - startIdx):
        if (row not in value_cnt.index):
            rowOffsets.append(rowOffsets[row])
        else:
            rowOffsets.append(rowOffsets[row]+value_cnt[row])
    return rowOffsets

def split_to_blocks(df, n_blocks, rows_name, cols_name):
    n_rows = df[rows_name].max()
    n_rows_in_block = n_rows / n_blocks
    for i in xrange(0, n_blocks):
        min_row = i*n_rows_in_block
        max_row = min_row + n_rows_in_block
        if i == n_blocks - 1 and max_row != n_rows:
            max_row = n_rows
        print rows_name + str(i + 1) + ' : ' + str(min_row + 1) + ', ' + str(max_row)
        df_block = df.loc[df[rows_name].isin(range(min_row + 1, max_row + 1))].sort([rows_name, cols_name]).reset_index(
            drop=True)
        rowOffsets = get_offsets(df_block[rows_name], min_row + 1, max_row + 1)

        with open(rows_name + str(i+1) + '.csv', 'wb') as csvfile:
            csr_writer = csv.writer(csvfile, delimiter=',')
            csr_writer.writerow(rowOffsets)
            csr_writer.writerow(df_block[cols_name].values)
            csr_writer.writerow(df_block['rating'].values)

user_name = 'nf_user_'
product_name = 'nf_product_'
df = pd.read_csv('nf_prize_total2.txt', names=[user_name, product_name, 'rating', 'date'])

n_blocks = 64
split_to_blocks(df, n_blocks, user_name, product_name)
split_to_blocks(df, n_blocks, product_name, user_name)


import os
from process import process_batch_file

input_file = os.path.abspath('queries.txt')
output_file = os.path.abspath('results.txt')
process_batch_file(input_file, output_file)

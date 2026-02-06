import pyreadstat
import pandas as pd

# Step 1: Read the SAS file
# Replace 'input_file.sas7bdat' with the path to your SAS dataset
df, meta = pyreadstat.read_sas7bdat("sys_evt.sas7bdat")

# Step 2: Export to CSV
# Replace 'output_file.csv' with your desired output filename
df.to_csv("sys_evt.csv", index=False)

print("Conversion complete! CSV saved as sys_qt")
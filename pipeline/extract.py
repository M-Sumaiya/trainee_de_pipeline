import pandas as pd

# Read a CSV file in chunks to handle large datasets.Returns a single DataFrame for simplicity.
def extract_csv(file_path, chunksize=100_000):
    chunks = pd.read_csv(file_path, chunksize=chunksize)
    return pd.concat(chunks, ignore_index=True)

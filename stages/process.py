import pandas as pd, pathlib, zipfile, os, shutil

brickdir = pathlib.Path('brick')
brickdir.mkdir(parents=True, exist_ok=True)

tmpdir = pathlib.Path('temp')
tmpdir.mkdir(parents=True, exist_ok=True)

zipfile.ZipFile('resources/CoMPAIT_SharedFolder.zip').extractall(tmpdir)

mdata = tmpdir / 'CoMPAIT_SharedFolder' / 'ModelingData'
for csv_file in mdata.glob('*.csv'):
    df = pd.read_csv(csv_file)
    df.to_parquet(brickdir / f"{csv_file.stem}.parquet")

# cleanup
shutil.rmtree(tmpdir)

# test that there are rows and columns in the output parquet files
for parquet_file in brickdir.glob('*.parquet'):
    df = pd.read_parquet(parquet_file)
    assert not df.empty, f"Output file {parquet_file} is empty"
    assert df.shape[1] > 0, f"Output file {parquet_file} has no columns"
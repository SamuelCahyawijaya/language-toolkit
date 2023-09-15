import os, sys
import request
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from unicodedata import normalize

# Download Manager
def download_iso639_3_map(out_dir='./', out_name=None):
    url = f'https://iso639-3.sil.org/sites/iso639-3/files/downloads/iso-639-3_Name_Index.tab'
    if out_name is None:
        out_path = f'{out_dir}/iso-639-3.tab'
    else:
        out_path = f'{out_dir}/{out_name}'
        
    r = requests.get(url, allow_redirects=True, stream=True)
    with open(out_path, 'wb') as file:
        for chunk in tqdm(r.iter_content(chunk_size=1024)):
            if chunk:
                file.write(chunk)
    return out_path

def download_wiki_iso_map(out_dir='./', out_name=None):
    if out_name is None:
        out_path = f'{out_dir}/wiki_iso_mapping.csv'
    else:
        out_path = f'{out_dir}/{out_name}'
    
    wiki_iso_df = pd.read_html('https://en.wikipedia.org/wiki/List_of_ISO_639-2_codes')[0]
    wiki_iso_df.columns = ['639-2', '639-3', '639-5', '639-1', 'lang_name', 'scope', 'type', 'native_name', 'other_name']

    wiki_iso_df.loc[pd.isna(wiki_iso_df['639-3']), '639-3'] = wiki_iso_df.loc[pd.isna(wiki_iso_df['639-3']), '639-2'].reset_index(drop=True)

    wiki_iso_df[['639-1', '639-3', 'lang_name', 'native_name', 'other_name', 'scope', 'type']]
    wiki_iso_df.to_csv(f'{out_dir}/wiki_iso_mapping.csv', index=False)
    return out_path

def download_iso639_2_map(out_dir='./', out_name=None):
    url = f'https://www.loc.gov/standards/iso639-2/php/code_list.php'
    if out_name is None:
        out_path = f'{out_dir}/iso-639-2.tab' 
    else:
        out_path = f'{out_dir}/{out_name}'

    r = requests.get(url, allow_redirects=True, stream=True)
    with open(out_path, 'wb') as file:
        for chunk in tqdm(r.iter_content(chunk_size=1024)):
            if chunk:
                file.write(chunk)
    return out_path

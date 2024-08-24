import argparse
import string
import sys
import os
import json
import time
import pandas as pd
import requests
from tqdm import tqdm
import zipfile

# Download Manager
def download_panlex_resources(version, out_dir='./'):
    # Check the version from https://db.panlex.org
    url = f'https://db.panlex.org/panlex-{version}-csv.zip'
    out_path = f'{out_dir}/panlex-{version}-csv.zip'
    
    r = requests.get(url, allow_redirects=True, stream=True)
    with open(out_path, 'wb') as file:
        for chunk in tqdm(r.iter_content(chunk_size=1024)):
            if chunk:
                file.write(chunk)
    return out_path
                
def extract_panlex_resources(panlex_zip_path, out_dir='./'):
    with zipfile.ZipFile(panlex_zip_path, 'r') as zip_ref:
        zip_ref.extractall(out_dir)

def download_and_extract_panlex_resources(version, out_dir='./'):
    zip_path = download_panlex_resources(version, out_dir=out_dir)
    extract_panlex_resources(zip_path, out_dir=out_dir)
    
# Data Loading Function
def load_panlex_resources(panlex_dir):
    # Load langvar
    langvar_df = pd.read_csv(f'{panlex_dir}/langvar.csv', keep_default_na=False)
    
    # Load expression
    expr_df = pd.read_csv(f'{panlex_dir}/expr.csv', keep_default_na=False)
    expr_df = expr_df.set_index('id')

    # Load denotation
    deno_df = pd.read_csv(f'{panlex_dir}/denotation.csv', keep_default_na=False)
    
    return langvar_df, expr_df, deno_df

# PanLex Functions
def get_langvar(lang_code, langvar_df):
    return langvar_df.loc[(langvar_df['lang_code'] == lang_code) & (langvar_df['var_code'] == 0),'id'].values[0]

def extract_monolingual_lexicon(lang, langvar_df, expr_df):
    # Get langvar id
    langvar = get_langvar(lang, langvar_df)
    
    # Extract terms from expression data
    terms = set()
    replacement_rules = str.maketrans('', '', string.punctuation)
    for item in expr_df.itertuples():
        if item.langvar == langvar:
            if pd.isna(item.txt):
                continue

            for token in item.txt.lower().translate(replacement_rules).split(' '):
                if len(token) > 0:
                    terms.add(token)
    terms = list(terms)
    
    # Return terms
    return pd.DataFrame({lang: terms})

def extract_bilingual_lexicon(lang_1, lang_2, langvar_df, expr_df, deno_df):
    # Get langvar id
    langvar_1 = get_langvar(lang_1, langvar_df)
    langvar_2 = get_langvar(lang_2, langvar_df)
    
    # Extract expression
    expr_dict = {}
    for item in expr_df.itertuples():
        if item.langvar in [langvar_1, langvar_2]:
            expr_dict[item.Index] = item.txt

    # Retrieve bilingual mapping based on meaning in denotation data
    mention_dict = {}
    expr_ids = list(expr_dict.keys())
    deno_filt_df = deno_df.loc[deno_df['expr'].isin(expr_ids),:]
    for item in deno_filt_df.itertuples():
        meaning_id, expr_id = item.meaning, item.expr
        expr_langvar = expr_df.loc[expr_id, 'langvar']
        if expr_langvar == langvar_1:
            if meaning_id in mention_dict:
                mention_dict[meaning_id][0].append(expr_id)
            else:
                mention_dict[meaning_id] = [[expr_id],[]]
        elif expr_langvar == langvar_2:
            if meaning_id in mention_dict:
                mention_dict[meaning_id][1].append(expr_id)
            else:
                mention_dict[meaning_id]=[[],[expr_id]]

    # Extract bilingual lexicon from bilingual mapping
    data_dict = {lang_1: [], lang_2: []}
    for key, exprs_pair in mention_dict.items():
        t1, t2 = [], []
        for expr_1 in exprs_pair[0]:
            t1.append(expr_dict[expr_1])
        for expr_2 in exprs_pair[1]:
            t2.append(expr_dict[expr_2])

        if t1!=[] and t2!=[]:
            for src_term in t1:
                for tgt_term in t2:
                    data_dict[lang_1].append(src_term)
                    data_dict[lang_2].append(tgt_term)
                    
    return pd.DataFrame(data_dict)

if __name__ == '__main__':
    # Test loading resource
    print('loading panlex resources...')
    stime = time.time()

    panlex_dir = 'panlex-20230501-csv'
    langvar_df, expr_df, deno_df = load_panlex_resources(panlex_dir)

    print(f'finished loading panlex resources in {time.time() - stime:.2f}s')

    # Test extract monolingual
    print('extracting monolingual lexicon (ind)...')
    stime = time.time()

    monolingual_lexicon = extract_monolingual_lexicon('ind', langvar_df, expr_df)
    display(monolingual_lexicon.head(10))

    print(f'finished extracting monolingual lexicon (ind) in {time.time() - stime:.2f}s')

    # Test extract bilingual
    print('extracting bilingual lexicon (ind-eng)...')
    stime = time.time()

    bilingual_lexicon = extract_bilingual_lexicon('ind', 'eng', langvar_df, expr_df, deno_df)
    display(bilingual_lexicon.head())

    print(f'finished extracting bilingual lexicon (ind-end) in {time.time() - stime:.2f}s')
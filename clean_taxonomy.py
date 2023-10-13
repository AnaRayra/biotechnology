from taxonomy_ranks import TaxonomyRanks
import json
import requests
from bs4 import BeautifulSoup
from collections import ChainMap
import asyncio
import re
import argparse


already_consuted = {
    "MB-A2-108":"genus",
    "Crenarchaeota": "class",
    "Candidatus Nitrocosmicus": "genus",
    "Bacteroidetes bacterium GWF2": "species",
    "Candidatus Heimdallarchaeota archaeon LC": "species",
    "Terriglobus roseus DSM": "species",
    "Candidatus Fischerbateria": "phylum",
}

def search_rank(name):
   rank_taxon = TaxonomyRanks(name)
   try:
      rank_taxon.get_lineage_taxids_and_taxanames()
   except Exception as e:
        print(str(e))
        return []
   potential_rank = []
   for potential_taxid in rank_taxon.lineages:
      for item, value in rank_taxon.lineages[potential_taxid].items():
         if value != ('NA', 'NA') and item not in ['user_taxa', 'taxa_searched'] and (value[0].lower() in name.lower()) :
            potential_rank.append(item)
   return potential_rank

async def get_taxonomy(url, name):
    if name in already_consuted.keys():
        return {already_consuted[name]: name} if already_consuted[name] != 'Not found' else name
    try:
        result = requests.post(url)
        soup = BeautifulSoup(result.content, "html.parser")
        element = re.findall(r'Rank: (.*?)Genetic', soup.get_text())
        if element and len(element) > 0:
            already_consuted[name] = element[0]
            rank = element[0]
            return {rank: name}
        else:
            already_consuted[name] = 'Not found'
            return name
    except Exception as e:
        print(str(e))
        return name

def treat_column(column):
    pattern_order = r' [0-9](.*)'
    column = column.split('__')[-1].replace('_', ' ').replace('\\n', '')
    return re.sub(pattern_order, '', column)

async def main(columns):
    fixed_cols = {
        'domain': columns[0].split('__')[-1].split('_')[-1]
    }
    tasks = []
    unknow = []
    for column in columns[1:]:
        if column.split('__')[-1].replace('_', ' ')[0].isdigit() or ('uncultured' in column or 'metagenome' in column or 'Subgroup' in column):
            continue
        column = treat_column(column)
        url = f'https://www.ncbi.nlm.nih.gov/Taxonomy/Browser/wwwtax.cgi?mode=Undef&name={column}&srchmode=1&keep=1&unlock=1&a=Go&mode=Undef&old_id=0&lvl=0&filter=',
        retorno = await get_taxonomy(url[0], column)
        if isinstance(retorno, dict):
            tasks.append(retorno)
        else:
            unknow.append(retorno)
    tasks.append({'unknow': str(unknow)})

    tasks.append(fixed_cols)
    return dict(ChainMap(*tasks))

def main(file, name):
    with open(file) as f:
        lines = f.readlines()
        records_bacteria = []
        records_eukaryoto = []
        n =0
        a=0
        loop = asyncio.get_event_loop()
        for line in lines:
            a=a+1
            columns = line.split(';')
            if 'Eukaryota' not in columns[0]:
                result = loop.run_until_complete(main(columns))
                records_bacteria.append(result)
            else:
                result = loop.run_until_complete(main(columns))
                records_eukaryoto.append(result)

        json_object_bac = json.dumps(records_bacteria)
        json_object_euc = json.dumps(records_eukaryoto)

        with open(f"{name}_bacteria_archea.json", "w") as outfile:
           outfile.write(json_object_bac)
        with open(f"{name}_eukaryota.json", "w") as outfile:
           outfile.write(json_object_euc)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-n", "--name", help="Nome da amostra")
    parser.add_argument("-f", "--file", help="Caminho para a amostra")

    args = parser.parse_args()
    main(args.name, args.file)

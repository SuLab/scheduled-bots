# downloads gene/protein json docs from mygene
# and also metdata from mygene
import os
from datetime import datetime
import requests
from mygene import MyGeneInfo
import json

class Downloader:

    def get_metadata(self):
        raise NotImplementedError("Needs to be implemented")

    def get_timestamp(self):
        # get the timestamp from mygene's metadata endpoint
        metadata = self.get_metadata()
        return datetime.strptime(metadata["build_version"], "%Y%m%d")

    def get_mg_cursor(self, taxid, filter_f=None):
        raise NotImplementedError("Needs to be implemented")

    def query(self):
        raise NotImplementedError("Needs to be implemented")

    def get_mg_gene(self, entrezgene):
        raise NotImplementedError("Needs to be implemented")

    def get_filter(self):
        raise NotImplementedError("Needs to be implemented")

    def get_key_source(self):
        raise NotImplementedError("Needs to be implemented")

class LocalDownloader(Downloader):

    def __init__(self, path):
        data = json.load(path)
        self.genes = data[0]
        self.metadata = data[1]
        self.sources = data[2]

    def get_filter(self):
        return lambda x: ("locus_tag" in x.keys())

    def get_mg_gene(self, entrezgene):
        for gene in self.data:
            if 'entrezgene' in gene.keys() and gene['entrezgene'] == entrezgene:
                return gene, 1

    def get_mg_cursor(self, taxid=None, filter_f=None):
        # get a list of all genes in the local file
        # accepts a function that can be used to filter the gene cursor (returns True or False for each doc)
        data = self.genes
        if filter_f:
            data = list(filter(filter_f, data))

        return data, len(data)

    def get_metadata(self):
        return self.metadata

    def get_key_source(self):
        return self.sources

class MyGeneDownloader(Downloader):
    def __init__(self, url="http://mygene.info/v3/", q=None, fields=None):
        self.base_url = url
        self.q = "__all__" if not q else q
        self.entrezonly = True
        self.fields = "entrezgene,ensembl,locus_tag,genomic_pos,name,other_names,symbol,uniprot,refseq,taxid," + \
                      "type_of_gene,genomic_pos_hg19,HGNC,homologene,MGI,RGD,SGD,FLYBASE,WormBase,ZFIN,BGD," + \
                      "alias,map_location" if not fields else fields
        # self.fields = ",".join(self.fields) if not isinstance(self.fields, str) else self.fields

    def get_filter(self):
        return lambda x: (x.get("type_of_gene") != "biological-region") and ("entrezgene" in x)

    def get_key_source(self):
        return {
            'SGD': 'entrez',
            'HGNC': 'entrez',
            'MIM': 'entrez',
            'FLYBASE': 'entrez',
            'WormBase': 'entrez',
            'ZFIN': 'entrez',
            'RGD': 'entrez',
            'MGI': 'entrez',
            'exons': 'ucsc',
            'ensembl': 'ensembl',
            'entrezgene': 'entrez',
            'genomic_pos': None,
            'genomic_pos_hg19': None,
            'locus_tag': 'entrez',
            'name': 'entrez',
            'symbol': 'entrez',
            'taxid': 'entrez',
            'type_of_gene': 'entrez',
            'refseq': 'entrez',
            'uniprot': 'uniprot',
            'homologene': 'entrez',
            'other_names': 'entrez',
            'alias': 'entrez',
            'map_location': 'entrez',
        }

    def get_metadata(self):
        r = requests.get(os.path.join(self.base_url, 'metadata'))
        r.raise_for_status()
        return r.json()

    def get_timestamp(self):
        # get the timestamp from mygene's metadata endpoint
        metadata = self.get_metadata()
        return datetime.strptime(metadata["build_version"], "%Y%m%d")

    def get_mg_cursor(self, taxid, filter_f=None):
        # get a cursor to all mygene docs for a specific taxid
        # accepts a function that can be used to filter the gene cursor (returns True or False for each doc)
        mg = MyGeneInfo(url=self.base_url)
        # get the total
        q = mg.query(self.q, fields=self.fields, species=str(taxid),
                     entrezonly=self.entrezonly)
        total = q['total']
        # get the cursor
        q = mg.query(self.q, fields=self.fields, species=str(taxid),
                     fetch_all=True, entrezonly=self.entrezonly)
        if filter_f:
            q = filter(filter_f, q)

        return q, total

    def query(self):
        mg = MyGeneInfo(url=self.base_url)
        # get the total
        q = mg.query(self.q, fields=self.fields, entrezonly=self.entrezonly)
        total = q['total']
        # get the cursor
        q = mg.query(self.q, fields=self.fields, fetch_all=True, entrezonly=self.entrezonly)
        return q, total


    def get_mg_gene(self, entrezgene):
        mg = MyGeneInfo(url=self.base_url)
        q = mg.getgene(entrezgene, fields=self.fields)
        return q, 1


if __name__ == "__main__":
    # example usage
    mgd = MyGeneDownloader()
    q, total = mgd.get_mg_cursor(9606, lambda x: x.get("type_of_gene") != "biological-region")
    print(next(q))
    print(total)

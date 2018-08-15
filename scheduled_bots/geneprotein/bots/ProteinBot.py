from tqdm import tqdm
import datetime

from scheduled_bots import PROPS
from scheduled_bots.geneprotein.helpers.DeprecatedStatements import remove_deprecated_statements
from wikidataintegrator import wdi_core, wdi_helpers
from wikidataintegrator.wdi_fastrun import FastRunContainer
from wikidataintegrator.wdi_helpers import format_msg, wait_for_last_modified
from scheduled_bots.geneprotein.helpers.qid_helper import get_qid_from_refseq

from scheduled_bots.geneprotein.bots.Bot import Bot
from scheduled_bots.geneprotein.protein.Protein import Protein
from wikidataintegrator.wdi_helpers import id_mapper

FASTRUN_PROPS = {'Entrez Gene ID', 'encodes', 'OMIM ID', 'Ensembl Protein ID', 'encoded by', 'instance of',
                 'found in taxon', 'Mouse Genome Informatics ID', 'Saccharomyces Genome Database ID',
                 'RefSeq Protein ID', 'UniProt ID'}


class ProteinBot(Bot):
    """
    Generic proteinbot class
    """
    failed = []  # list of uniprot ids for those that failed
    to_encode = []

    def __init__(self, organism_info, login):
        super().__init__(login)
        self.organism_info = organism_info
        self.uniprot_qid = dict()

    def run(self, records, total=None, write=True, refseq=False, fast_run=True):
        records = self.filter(records)
        entrez_mapping = id_mapper("P351", (("P703", self.organism_info['wdid']),))
        locus_mapping = id_mapper("P2393", (("P703", self.organism_info['wdid']),))
        print("Creating Protein Item(s)")
        for record in tqdm(records, mininterval=2, total=total):

            entrez = str(record['entrezgene']['@value'])
            locus = str(record['locus_tag']['@value'])
            if entrez and entrez in entrez_mapping:
                gene_wdid = entrez_mapping[entrez]
            elif locus and locus in locus_mapping:
                gene_wdid = locus_mapping[locus]

            else:
                value = str(record['entrezgene']['@value'])
                wdi_core.WDItemEngine.log("WARNING", format_msg(value, "P351", None,
                                                                "Gene item not found during protein creation", None))
                continue


            # handle multiple protiens
            if 'uniprot' in record and 'Swiss-Prot' in record['uniprot']['@value']:
                uniprots = record['uniprot']['@value']['Swiss-Prot']
                for uniprot in uniprots:
                    record['uniprot']['@value']['Swiss-Prot'] = uniprot
                    self.run_one(record, gene_wdid, write, refseq=refseq, fast_run=fast_run)
            else:
                self.run_one(record, gene_wdid, write, refseq=refseq, fast_run=fast_run)

        wait_for_last_modified(datetime.datetime.now())
        print("Updating gene encodes statements")
        for protein in tqdm(self.to_encode, mininterval=2, total=total):
            protein.make_gene_encodes(fast_run=fast_run, write=write, refseq=refseq)
            if protein.status is not True:
                self.failed.append(protein.uniprot)

    def run_one(self, record, gene_wdid, write, fast_run=True, refseq=False):
        protein = Protein(record, self.organism_info, gene_wdid, self.login)
        try:
            protein.parse_external_ids()
            uniprot = protein.external_ids['UniProt ID']
            refseq_id = protein.external_ids['RefSeq Protein ID']
        except Exception as e:
            msg = wdi_helpers.format_msg(gene_wdid, None, None, str(e), msg_type=type(e))
            wdi_core.WDItemEngine.log("ERROR", msg)
            return

        # some proteins are encoded by multiple genes. don't try to create it again
        if uniprot and uniprot in self.uniprot_qid:
            qid = self.uniprot_qid[uniprot]
            wait_for_last_modified(datetime.datetime.now())
            wditem = protein.update_item(qid, fast_run=fast_run, write=write, refseq=refseq)
        else:

            # check if it can be found with refseq
            if not uniprot:
                qid = get_qid_from_refseq(refseq_id[0], record['taxid']['@value'])
                if qid:
                    wait_for_last_modified(datetime.datetime.now())
                    wditem = protein.update_item(qid, fast_run=fast_run, write=write, refseq=refseq)
                else:
                    wditem = protein.create_item(fast_run=fast_run, write=write, refseq=refseq)
            else:
                wditem = protein.create_item(fast_run=fast_run, write=write, refseq=refseq)
        if wditem is not None:
            if uniprot:
                self.uniprot_qid[uniprot] = wditem.wd_item_id
            self.to_encode.append(protein)

    def filter(self, records):
        """
        This is used to selectively skip certain records based on conditions within the record or to specifically
        alter certain fields before sending to the Bot
        """
        # If we are processing zebrafish records, skip the record if it doesn't have a zfin ID
        for record in records:
            if record['taxid']['@value'] == 7955 and 'ZFIN' not in record:
                continue
            else:
                yield record

    def cleanup(self, releases, last_updated):
        print(self.failed)
        uniprot_wdid = wdi_helpers.id_mapper(PROPS['UniProt ID'],
                                             ((PROPS['found in taxon'], self.organism_info['wdid']),))
        print(len(uniprot_wdid))
        uniprot_wdid = {uniprot: qid for uniprot, qid in uniprot_wdid.items() if uniprot not in self.failed}
        print(len(uniprot_wdid))
        filter = {PROPS['UniProt ID']: '', PROPS['found in taxon']: self.organism_info['wdid']}
        frc = FastRunContainer(wdi_core.WDBaseDataType, wdi_core.WDItemEngine, base_filter=filter, use_refs=True)
        frc.clear()
        props = [PROPS[x] for x in FASTRUN_PROPS]
        for qid in tqdm(uniprot_wdid.values()):
            remove_deprecated_statements(qid, frc, releases, last_updated, props, self.login)

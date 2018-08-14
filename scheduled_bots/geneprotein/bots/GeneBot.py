from .Bot import Bot
import sys
import traceback

from tqdm import tqdm
from scheduled_bots.geneprotein.helpers.ReferenceFactory import ReferenceFactory

from scheduled_bots import PROPS
from wikidataintegrator import wdi_core, wdi_helpers
from wikidataintegrator.wdi_fastrun import FastRunContainer
from scheduled_bots.geneprotein.helpers.DeprecatedStatements import remove_deprecated_statements

from scheduled_bots.geneprotein.gene.HumanGene import HumanGene
from scheduled_bots.geneprotein.gene.MicrobeGene import MicrobeGene
from scheduled_bots.geneprotein.gene.ChromosomalGene import ChromosomalGene

FASTRUN_PROPS = {'Entrez Gene ID', 'strand orientation', 'Saccharomyces Genome Database ID', 'RefSeq RNA ID',
                 'ZFIN Gene ID', 'Ensembl Transcript ID', 'HGNC ID', 'encodes', 'genomic assembly', 'found in taxon',
                 'HomoloGene ID', 'MGI Gene Symbol', 'cytogenetic location', 'Mouse Genome Informatics ID',
                 'FlyBase Gene ID', 'genomic end', 'NCBI Locus tag', 'Rat Genome Database ID', 'Ensembl Gene ID',
                 'instance of', 'chromosome', 'HGNC Gene Symbol', 'Wormbase Gene ID', 'genomic start'}


class GeneBot(Bot):
    """
    Generic genebot class
    """
    item = None
    failed = []  # list of entrez ids for those that failed

    def __init__(self, organism_info, login):
        super().__init__(login)
        self.organism_info = organism_info

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
        """

        :param releases:
        :param last_updated:
        :param failed: list of entrez ids to skip
        :return:
        """
        print(self.failed)
        entrez_qid = wdi_helpers.id_mapper('P351', ((PROPS['found in taxon'], self.organism_info['wdid']),))
        print(len(entrez_qid))
        entrez_qid = {entrez: qid for entrez, qid in entrez_qid.items() if entrez not in self.failed}
        print(len(entrez_qid))
        filter = {PROPS['Entrez Gene ID']: '', PROPS['found in taxon']: self.organism_info['wdid']}
        frc = FastRunContainer(wdi_core.WDBaseDataType, wdi_core.WDItemEngine, base_filter=filter, use_refs=True)
        frc.clear()
        props = [PROPS[x] for x in FASTRUN_PROPS]
        for qid in tqdm(entrez_qid.values()):
            remove_deprecated_statements(qid, frc, releases, last_updated, props, self.login)


class ChromosomalGeneBot(GeneBot):
    GENE_CLASS = ChromosomalGene

    def __init__(self, organism_info, chr_num_wdid, login):
        super().__init__(organism_info, login)
        self.chr_num_wdid = chr_num_wdid

    def run(self, records, total=None, fast_run=True, write=True, refseq=False, deprecated_entrez=False):
        records = self.filter(records)
        for record in tqdm(records, mininterval=2, total=total):
            gene = self.GENE_CLASS(record, self.organism_info, self.chr_num_wdid, ReferenceFactory(self.login))
            try:
                item = gene.create_item(fast_run=fast_run, refseq=refseq, deprecated_entrez=deprecated_entrez)
                id, prop = gene.get_id_and_prop()
                status = wdi_helpers.try_write(item, id, prop, self.login, write=write)
            except Exception as e:
                exc_info = sys.exc_info()
                traceback.print_exception(*exc_info)
                id, prop = gene.get_id_and_prop()
                msg = wdi_helpers.format_msg(id, prop, None, str(e), msg_type=type(e))
                wdi_core.WDItemEngine.log("ERROR", msg)
                status = msg
            if status is not True:
                self.failed.append(gene.entrez)


class HumanGeneBot(ChromosomalGeneBot):
    GENE_CLASS = HumanGene


class MicrobeGeneBot(ChromosomalGeneBot):
    GENE_CLASS = MicrobeGene

from wikidataintegrator import wdi_core, wdi_helpers
from wikidataintegrator.wdi_helpers import format_msg

INTERPRO = "P2926"
INSTANCE_OF = "P31"

class IPRTerm:
    """
    Represents one interproscan term/item

    {'children': ['IPR020635'],
     'contains': ['IPR001824', 'IPR002011', 'IPR008266', 'IPR017441'],
     'description': 'InterPro Domain',
     'found_in': ['IPR009136','IPR012234','IPR020777'],
     'id': 'IPR001245',
     'name': 'Serine-threonine/tyrosine-protein kinase catalytic domain',
     'parent': 'IPR000719',
     'short_name': 'Ser-Thr/Tyr_kinase_cat_dom',
     'type': 'Domain',
     'type_wdid': 'Q898273'}

    """
    fast_run_base_filter = {INTERPRO: ''}
    ipr2wd = wdi_helpers.id_mapper(INTERPRO)

    type2desc = {"Active_site": "InterPro Active Site",
                 "Binding_site": "InterPro Binding Site",
                 "Conserved_site": "InterPro Conserved Site",
                 "Domain": "InterPro Domain",
                 "Family": "InterPro Family",
                 "PTM": "InterPro PTM",
                 "Repeat": "InterPro Repeat"}
    type2wdid = {"Active_site": "Q423026",  # Active site
                 "Binding_site": "Q616005",  # Binding site
                 "Conserved_site": "Q7644128",  # Supersecondary_structure
                 "Domain": "Q898273",  # Protein domain
                 "Family": "Q417841",  # Protein family
                 "PTM": "Q898362",  # Post-translational modification
                 "Repeat": "Q3273544"}  # Structural motif

    def __init__(self, name=None, short_name=None, id=None, parent=None, children=None, contains=None,
                 found_in=None, type=None, description=None, release_wdid=None, **kwargs):
        self.name = name
        self.short_name = short_name
        self.id = id
        self.wdid = None
        self.parent = parent  # subclass of (P279)
        self.parent_wdid = None
        self.children = children  # not added to wd
        self.children_wdid = None
        self.contains = contains  # has part (P527)
        self.contains_wdid = None
        self.found_in = found_in  # part of (P361)
        self.found_in_wdid = None
        self.type = type
        self.type_wdid = IPRTerm.type2wdid[self.type]  # subclass of (from type2wdid)
        self.description = description
        if self.description is None and self.type:
            self.description = IPRTerm.type2desc[self.type]
        self.lang_descr = {'en': self.description}
        self.release_wdid = release_wdid
        self.reference = None
        self.create_reference()

    def __repr__(self):
        return '{}: {}'.format(self.id, self.name)

    def __str__(self):
        return '{}: {}'.format(self.id, self.name)

    @classmethod
    def refresh_ipr_wd(cls):
        cls.ipr2wd = wdi_helpers.id_mapper(INTERPRO)

    def do_wdid_lookup(self):
        # this can only be done after all items have been created
        self.wdid = IPRTerm.ipr2wd[self.id]
        if self.parent:
            self.parent_wdid = IPRTerm.ipr2wd[self.parent]
        # children aren't added (reverse of parent relationship)
        if self.contains:
            self.contains_wdid = [IPRTerm.ipr2wd[x] for x in self.contains]
        if self.found_in:
            self.found_in_wdid = [IPRTerm.ipr2wd[x] for x in self.found_in]

    def create_reference(self):
        """ Create wikidata references for interpro
        This same reference will be used for everything. Except for a ref to the interpro item itself
        """
        # stated in Interpro version XX.X
        ref_stated_in = wdi_core.WDItemID(self.release_wdid, 'P248', is_reference=True)
        ref_ipr = wdi_core.WDString(self.id, INTERPRO, is_reference=True)  # interpro ID
        self.reference = [ref_stated_in, ref_ipr]

    def create_item(self, login=None, fast_run=True, write=True):
        # if no login given, write will not be attempted
        statements = [wdi_core.WDExternalID(value=self.id, prop_nr=INTERPRO, references=[self.reference]),
                      wdi_core.WDItemID(value=self.type_wdid, prop_nr=INSTANCE_OF,
                                        references=[self.reference])]

        wd_item = wdi_core.WDItemEngine(item_name=self.name, domain='interpro', data=statements,
                                        append_value=["P279"],
                                        fast_run=fast_run, fast_run_base_filter=IPRTerm.fast_run_base_filter)
        wd_item.set_label(self.name, lang='en')
        for lang, description in self.lang_descr.items():
            wd_item.set_description(description, lang=lang)
        wd_item.set_aliases([self.short_name, self.id])

        if login:
            wdi_helpers.try_write(wd_item, self.id, INTERPRO, login, write=write)

        return wd_item

    def create_relationships(self, login, write=True):
        try:
            # endpoint may not get updated in time?
            self.do_wdid_lookup()
        except KeyError as e:
            wdi_core.WDItemEngine.log("ERROR", format_msg(self.id, INTERPRO, None, str(e), type(e)))
            return

        statements = [wdi_core.WDExternalID(value=self.id, prop_nr=INTERPRO, references=[self.reference])]
        if self.parent:
            # subclass of
            statements.append(wdi_core.WDItemID(value=self.parent_wdid, prop_nr='P279', references=[self.reference]))
        if self.contains:
            for c in self.contains_wdid:
                statements.append(wdi_core.WDItemID(value=c, prop_nr='P527', references=[self.reference]))  # has part
        if self.found_in:
            for f in self.found_in_wdid:
                statements.append(wdi_core.WDItemID(value=f, prop_nr='P361', references=[self.reference]))  # part of
        if len(statements) == 1:
            return

        wd_item = wdi_core.WDItemEngine(wd_item_id=self.wdid, domain='interpro', data=statements,
                                        append_value=['P279', 'P527', 'P361'],
                                        fast_run=True, fast_run_base_filter=IPRTerm.fast_run_base_filter)

        wdi_helpers.try_write(wd_item, self.id, INTERPRO, login, edit_summary="create/update subclass/has part/part of", write=write)

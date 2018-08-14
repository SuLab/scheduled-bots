from scheduled_bots.geneprotein.HelperBot import make_ref_source

class ReferenceFactory:

    def __init__(self, login):
        self.login = login

    def get_reference(self, record_source, prop, id):
        return make_ref_source(record_source, prop, id, self.login)
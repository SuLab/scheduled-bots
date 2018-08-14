class Bot:

    def __init__(self, login):
        self.login = login

    def run(self, records):
        raise NotImplementedError("Run method for this bot not implemented")

    def filter(self, records):
        raise NotImplementedError("Filter method for this bot not implemented")

    def cleanup(self):
        raise NotImplementedError("Cleanup method for this bot not implemented")

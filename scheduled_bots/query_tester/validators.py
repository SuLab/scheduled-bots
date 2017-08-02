
class Validator:
    description = ''  # Plain text description of what is being checked
    expected_result = []  # optional

    def __init__(self):
        self.success = None  # True or False
        self.result_message = ''  # optional extra information about test result

    def validate(self, result):
        raise NotImplementedError("Implement a Validator Subclass")

class OneOrMoreResultsValidator(Validator):
    description = "Checks for at least 1 result"

    def validate(self, result):
        self.success = True if len(result) >= 1 else False

class NoResultsValidator(Validator):
    description = "Checks for no results"

    def validate(self, result):
        self.success = True if len(result) == 0 else False

class NoValidator(Validator):
    description = "No validation"

    def validate(self, result):
        self.success = None

class FailValidator(Validator):
    description = "Always returns FAIL"
    expected_result = [{'a': 4}]

    def validate(self, result):
        self.success = False
        self.result_message = "this is more info"
class DatasetMetadata:
    def __init__(self, attributes, types, rules):
        self.attributes = attributes
        self.types = types
        self.rules = rules

    def get_attributes(self):
        return self.attributes

    def get_types(self):
        return self.types

    def get_rules(self):
        return self.rules
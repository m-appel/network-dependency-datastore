import sys
from itertools import chain, combinations


class SetPermutator:

    def __init__(self):
        self.classes = dict()
        self.permutations = dict()
        self.permutations_computed = True

    def add_class(self, name: str, values: set) -> None:
        if name in self.classes:
            print(f'Error: Class {name} is already present.', file=sys.stderr)
            return
        self.classes[name] = values
        self.permutations_computed = False

    def __compute_permutations(self) -> None:
        self.permutations.clear()
        class_combinations = self.powerset(self.classes.items())
        for combination in class_combinations:
            if len(combination) == 0:
                print(f'Skipped combination {combination}')
                continue
            names, values = zip(*combination)
            # Build a set for all classes not in this combination to
            # calculate the difference later since we want exclusive
            # membership.
            not_in_class_names = list()
            not_in_class_values = set()
            for name in self.classes:
                if name not in names:
                    not_in_class_names.append(name)
                    not_in_class_values.update(self.classes[name])
            class_name = ' '.join(names)
            not_in_class_name = ' '.join(not_in_class_names)
            print(f'class: {class_name} nclass: {not_in_class_name}')
            class_values = set.intersection(*values) - not_in_class_values
            self.permutations[class_name] = class_values
        self.permutations_computed = True

    def get_permutations(self, class_size: int = None) -> dict:
        if not self.permutations_computed:
            self.__compute_permutations()
        if not class_size:
            return self.permutations
        selected_classes = {class_name: self.permutations[class_name]
                            for class_name in self.permutations.keys()
                            if len(class_name.split()) == class_size}
        return selected_classes

    @staticmethod
    def powerset(iterable):
        "powerset([1,2,3]) --> () (1,) (2,) (3,) (1,2) (1,3) (2,3) (1,2,3)"
        s = list(iterable)
        return chain.from_iterable(combinations(s, r) for r in range(len(s) + 1))
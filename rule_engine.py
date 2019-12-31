from collections import defaultdict


class Rule:
    def __init__(self, sources, target, action):
        self.sources = sources
        self.target = target
        self.action = action

    def __str__(self):
        sources = ",".join(str(s) for s in self.sources)
        target = str(self.target)
        action = str(self.action)
        return f"Rule([{sources}], {target}, {action})"


class RuleEngine:
    def __init__(self, rules):
        # For each destination this dict holds the best known
        # way to get there from various other locations.
        self.matrix = defaultdict(dict)

        # Put the rules in the matrix
        for rule in rules:
            t = rule.target
            for s in rule.sources:
                if not s in self.matrix[t]:
                    self.matrix[t][s] = (rule, 1)

        # Compute the closure of the rule matrix
        done = False
        while not done:
            done = True
            # Look at all route1 + route2 combinations.
            for intermediate, target, _, cost2 in self.all_routes():
                for source, _, rule, cost1 in self.all_routes_to(intermediate):
                    if source != target:
                        cost = cost1 + cost2
                        entry = self.matrix[target].get(source)
                        if not entry or entry[1] > cost:
                            self.matrix[target][source] = (rule, cost)
                            done = False

    def all_routes_to(self, target):
        result = []
        for source, (rule, cost) in self.matrix[target].items():
            result.append((source, target, rule, cost))
        return result

    def all_routes(self):
        result = []
        for target in self.matrix.keys():
            result.extend(self.all_routes_to(target))
        return result

    def pick_rule(self, start, finish):
        entry = self.matrix[finish].get(start)
        if entry:
            return entry[0]
        else:
            return None

    def plan(self, start, finish):
        rule = self.pick_rule(start, finish)
        if rule:
            return rule.action
        else:
            return None


if __name__ == '__main__':
    engine = RuleEngine([
        Rule(['STOPPED'], 'PENDING', "start"),
        Rule(['PENDING'], 'RUNNING', "wait"),
        Rule(['RUNNING'], 'READY', "wait"),
        Rule(['RUNNING', 'READY'], 'STOPPING', "stop"),
        Rule(['STOPPING'], 'STOPPED', "wait")
    ])

    def iterate(eng, start, finish):
        itinerary = [start]
        cur = start
        while cur != finish:
            rule = eng.pick_rule(cur, finish)
            if not rule:
                itinerary.append(None)
                break
            action = rule.action
            next = rule.target
            itinerary.append(rule.action)
            itinerary.append(next)
            cur = next
        return itinerary

    testcases = [
        ('STOPPED', 'READY'),
        ('READY', 'STOPPED'),
        ('STOPPING', 'READY'),
        ('PENDING', 'READY'),
        ('TERMINATED', 'READY'),
    ]

    for start, finish in testcases:
        print(start, finish, iterate(engine, start, finish))

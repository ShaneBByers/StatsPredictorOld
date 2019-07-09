import logging


class IntegerProgrammingSolver:

    def __init__(self, logger_name):
        self.objective_values = ()
        self.objective_extra = 0
        self.substitute_values = []
        self.constraints = []

        self.logger = logging.getLogger(logger_name)

    def set_objective(self, values):
        self.objective_values = values

    def add_constraint(self, values, rhs, is_equals=False):
        new_constraint = IntegerProgrammingConstraint()
        new_constraint.values = values
        new_constraint.rhs = rhs
        self.constraints.append(new_constraint)
        if is_equals:
            inverse_constraint = IntegerProgrammingConstraint()
            inverse_values = []
            for value in values:
                inverse_values.append(value * -1)
            inverse_constraint.values = inverse_values
            inverse_constraint.rhs = rhs * -1
            self.constraints.append(inverse_constraint)

    def fix_objective_values(self):
        new_objective_values = []
        for i in range(len(self.objective_values)):
            if self.objective_values[i] > 0:
                self.substitute_values.append(i)
                self.objective_extra += self.objective_values[i]
                new_objective_values.append(-1 * self.objective_values[i])
                for constraint in self.constraints:
                    constraint.substitute_value(i)
            else:
                new_objective_values.append(self.objective_values[i])
        self.objective_values = new_objective_values

    def solve(self):
        self.fix_objective_values()
        unfathomed_list = []
        best_result = None
        best_result_values = []

        unfathomed_list.append([])

        while len(unfathomed_list) > 0:
            fixed_list = unfathomed_list.pop(0)

            current_values = []
            for i, _ in enumerate(self.objective_values):
                found = False
                for fixed_list_item in fixed_list:
                    if i == fixed_list_item[0]:
                        current_values.append(fixed_list_item[1])
                        found = True
                        break
                if not found:
                    current_values.append(0)

            feasible = sum(current_values) == 9
            if feasible:
                for constraint in self.constraints:
                    if not constraint.check_feasible(current_values):
                        feasible = False
                        break

            if feasible:
                current_result = self.objective_extra
                for i, objective_values_item in enumerate(self.objective_values):
                    current_result += objective_values_item * current_values[i]

                if best_result is None or current_result > best_result:
                    best_result = current_result
                    best_result_values = current_values
            else:
                feasible_children = sum(current_values) < 9
                if feasible_children:
                    for constraint in self.constraints:
                        if not constraint.check_feasible_children(fixed_list):
                            feasible_children = False
                            break

                if feasible_children:
                    variable_to_be_fixed = None
                    least_variable_infeasibilities = None
                    for i, _ in enumerate(self.objective_values):
                        found = False
                        for fixed in fixed_list:
                            if i == fixed[0]:
                                found = True
                                break

                        if not found:
                            # variable_to_be_fixed = i
                            variable_infeasibilities = 0
                            for constraint in self.constraints:
                                variable_infeasibilities += constraint.check_infeasabilities(fixed_list)
                            if least_variable_infeasibilities is None or variable_infeasibilities < least_variable_infeasibilities:
                                least_variable_infeasibilities = variable_infeasibilities
                                variable_to_be_fixed = i

                    if variable_to_be_fixed is not None:
                        zero_node = [(variable_to_be_fixed, 0)]

                        one_node = [(variable_to_be_fixed, 1)]

                        for fixed in fixed_list:
                            zero_node.append(fixed)
                            one_node.append(fixed)
                        # Fix all other Centers to 0 if 2 Centers are currently fixed to 1
                        # Pass in Center indeces to IP Solver
                        # If number of Centers required = number of Centers not fixed, fix both to 1
                        # Don't add these to fixed list. Just append it to zero_/one_node and add it to unfathomed.

                        unfathomed_list.append(zero_node)
                        unfathomed_list.append(one_node)

        for i, best_result_values_item in enumerate(best_result_values):
            for substitute_index in self.substitute_values:
                if i == substitute_index:
                    best_result_values_item = 1 - best_result_values_item

        return {'VALUES': best_result_values,
                'RESULT': best_result}


class IntegerProgrammingConstraint:

    def __init__(self):
        self.values = ()
        self.rhs = None

    def substitute_value(self, sub_index):
        new_values = []
        for i in range(len(self.values)):
            if i == sub_index:
                new_values.append(-1 * self.values[i])
            else:
                new_values.append(self.values[i])
        self.values = new_values
        self.rhs += self.values[sub_index]

    def check_feasible(self, current_values):
        current_result = 0
        for i in range(len(self.values)):
            current_result += self.values[i] * current_values[i]

        return current_result <= self.rhs

    def check_feasible_children(self, fixed_values):
        current_result = 0
        for i in range(len(self.values)):
            found = False
            for fixed in fixed_values:
                if i == fixed[0]:
                    current_result += self.values[i] * fixed[1]
                    found = True
                    break
            if not found and self.values[i] < 0:
                current_result += self.values[i]

        return current_result <= self.rhs

    def check_infeasabilities(self, fixed_values):
        current_result = 0
        for i in range(len(self.values)):
            found = False
            for fixed in fixed_values:
                if i == fixed[0]:
                    current_result += self.values[i] * fixed[1]
                    found = True
                    break

        if current_result <= self.rhs:
            return 0
        else:
            return self.rhs - current_result


class LinearProgrammingSolver:

    def __init__(self, logger_name):
        self.objective_values = ()
        self.solving_objective_values = []
        self.solving_objective_rhs = None
        self.constraints = []

        self.logger = logging.getLogger(logger_name)

    def set_objective(self, values):
        self.objective_values = values

    def add_constraint(self, values, rhs):
        new_constraint = LinearProgrammingConstraint()
        new_constraint.values = values
        new_constraint.non_slack_count = len(values)
        new_constraint.rhs = rhs
        self.constraints.append(new_constraint)

    def log_current_state(self):
        self.logger.info("== START CURRENT STATE ==")
        objective_string = ""
        i = 0
        slack_count = 0
        for value in self.solving_objective_values[:-1]:
            if i < len(self.objective_values):
                objective_string += str(round(value, 2)) + "x[" + str(i) + "] + "
            else:
                objective_string += str(round(value, 2)) + "s[" + str(slack_count) + "] + "
                slack_count += 1
            i += 1
        else:
            objective_string += str(round(self.solving_objective_values[-1], 2)) + "P"
        objective_string += " = " + str(round(self.solving_objective_rhs, 2))
        for constraint in self.constraints:
            self.logger.info(constraint.get_current_state())
        self.logger.info(objective_string)
        self.logger.info("== END CURRENT STATE ==")

    def add_slack_variables(self):
        i = 0
        for outer_index in range(len(self.constraints)):
            for inner_index in range(len(self.constraints)):
                if outer_index == inner_index:
                    self.constraints[outer_index].add_slack_variable(1)
                else:
                    self.constraints[outer_index].add_slack_variable(0)

        for index in range(len(self.constraints)):
            self.constraints[index].add_slack_variable(0)

    def set_solving_objective_values(self):
        for value in self.objective_values:
            self.solving_objective_values.append(-1 * value)
        for _ in range(len(self.constraints)):
            self.solving_objective_values.append(0)
        self.solving_objective_values.append(1)
        self.solving_objective_rhs = 0

    def pivot_using_constraint(self, pivot_constraint, pivot_col):
        factor = -1 * self.solving_objective_values[pivot_col]

        new_values = []

        for index in range(len(self.solving_objective_values)):
            new_values.append(self.solving_objective_values[index] + factor * pivot_constraint.values[index])

        self.solving_objective_values = new_values
        self.solving_objective_rhs += factor * pivot_constraint.rhs

    def solve(self):
        self.add_slack_variables()
        self.set_solving_objective_values()
        self.log_current_state()

        negative_objective_values = False

        for value in self.solving_objective_values:
            if value < 0:
                negative_objective_values = True

        while negative_objective_values:
            pivot_col = None
            pivot_col_value = None
            i = 0
            for value in self.solving_objective_values:
                if pivot_col_value is None or value < pivot_col_value:
                    pivot_col_value = value
                    pivot_col = i
                i += 1

            if pivot_col is not None:
                self.logger.info("PIVOT COL: " + str(pivot_col))
                pivot_row = None
                pivot_row_value = None
                i = 0
                for constraint in self.constraints:
                    constraint_value = 0
                    if constraint.values[pivot_col] != 0:
                        constraint_value = constraint.get_pivot_row_value(pivot_col)
                    if constraint_value > 0 and (pivot_row_value is None or constraint_value < pivot_row_value):
                        pivot_row_value = constraint_value
                        pivot_row = i
                    i += 1

                if pivot_row is not None:
                    self.logger.info("PIVOT ROW: " + str(pivot_row))
                    pivot_value = self.constraints[pivot_row].values[pivot_col]
                    self.logger.info("PIVOT VALUE: " + str(pivot_value))
                    self.constraints[pivot_row].update_with_pivot_value(pivot_value)
                    self.log_current_state()

                    pivot_constraint_index = 0
                    for _ in range(len(self.constraints)):
                        if pivot_constraint_index != pivot_row:
                            self.constraints[pivot_constraint_index].pivot_using_constraint(self.constraints[pivot_row],
                                                                                            pivot_col)
                        pivot_constraint_index += 1
                    self.pivot_using_constraint(self.constraints[pivot_row],
                                                pivot_col)
            negative_objective_values = False
            for value in self.solving_objective_values:
                if value < 0:
                    negative_objective_values = True

        self.logger.info("== FINAL STATE == ")
        self.log_current_state()

        solution_values = {}

        for col_index in range(len(self.solving_objective_values)):
            for row_index in range(len(self.constraints)):
                if self.constraints[row_index].values[col_index] == 1:
                    basic = True
                    for other_row_index in range(len(self.constraints)):
                        if other_row_index != row_index:
                            if self.constraints[other_row_index].values[col_index] != 0:
                                basic = False

                    if basic:
                        solution_values[str(col_index)] = self.constraints[row_index].rhs

        solution_values["P"] = self.solving_objective_rhs

        return solution_values


class LinearProgrammingConstraint:

    def __init__(self):
        self.values = ()
        self.non_slack_count = 0
        self.rhs = None

    def get_current_state(self):
        log_string = ""
        i = 0
        slack_count = 0
        for value in self.values[:-1]:
            if i < self.non_slack_count:
                log_string += str(round(value, 2)) + "x[" + str(i) + "] + "
            else:
                log_string += str(round(value, 2)) + "s[" + str(slack_count) + "] + "
                slack_count += 1
            i += 1
        else:
            log_string += str(self.values[-1]) + "P"
        log_string += " = "
        log_string += str(round(self.rhs, 2))
        return log_string

    def get_pivot_row_value(self, pivot_col):
        return self.rhs / self.values[pivot_col]

    def update_with_pivot_value(self, pivot_value):
        factor = 1/pivot_value
        new_values = []
        for value in self.values:
            new_values.append(value * factor)
        self.values = new_values
        self.rhs *= factor

    def add_slack_variable(self, value):
        new_values = []
        for old_value in self.values:
            new_values.append(old_value)
        new_values.append(value)
        self.values = new_values

    def pivot_using_constraint(self, pivot_constraint, pivot_col):
        factor = -1 * self.values[pivot_col]

        new_values = []

        for index in range(len(self.values)):
            new_values.append(self.values[index] + factor * pivot_constraint.values[index])

        self.values = new_values
        self.rhs += factor * pivot_constraint.rhs

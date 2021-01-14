"""
A Python implementation of *compact* Dotted Version Vectors, which
provides a container for a set of concurrent values (siblings) with causal
order information.
"""
import functools
from numbers import Number


def foldl(func, acc, xs):
    """
    Erlang's implementation of lists:foldl/3
    """
    result = functools.reduce(func, reversed(xs), acc)
    return result


def cmp_fun(a, b):
    """
    Allows to compare lists with strings, as in Erlang.
    ( list > string )
    """
    if isinstance(a, str) and isinstance(b, str):
        return a > b
    if isinstance(a, Number) and isinstance(b, Number):
        return a > b
    if isinstance(a, list) and isinstance(b, list):
        if len(a) > 0 and len(b) > 0:
            if isinstance(a[0], list) and isinstance(b[0], list):
                return len(a[0]) > len(b[0])
            if isinstance(a[0], list):
                return True
    if isinstance(a, list) and not isinstance(b, list):
        return True
    if isinstance(b, list):
        return False
    return len(a) > len(b)


class Vector(list):
    """
    Vector object for type reference.
    """
    def __str__(self):
        return "Vector {}".format(str(self))


class Entries(list):
    """
    Entries object for type reference.
    """
    def __str__(self):
        return "Entries {}".format(str(self))


class Clock(list):
    """
    Clock object.
    * Entries are sorted by id
    * Each counter also includes the number of values in that id
    * The values in each triple of entries are causally ordered
      and each new value goes to the head of the list
    """
    def __init__(self, entries, values, *args, **kwargs):
        super(Clock, self).__init__(*args, **kwargs)
        self.append(entries)
        self.append(values)

    def _get_entries(self):
        return self[0]
    entries = property(_get_entries)

    def _get_values(self):
        return self[1]
    values = property(_get_values)



class DVVSet:
    """
    DVVSet helper object.
    """

    def new(self, value) -> Clock:
        """
        Constructs a new clock set without causal history,
        and receives one value that goes to the anonymous list.
        """
        return Clock([], [value])

    def new_list(self, value) -> Clock:
        """
        Same as new, but receives a list of values, instead of a single value.
        """
        if isinstance(value, list):
            return Clock([], value)
        return Clock([], [value])

    def new_with_history(self, vector: Vector, value) -> Clock:
        """
        Constructs a new clock set with the causal history
        of the given version vector / vector clock,
        and receives one value that goes to the anonymous list.
        The version vector SHOULD BE the output of join.
        """
        # defense against non-order preserving serialization
        vectors = sorted(vector, key=functools.cmp_to_key(cmp_fun))
        entries = []
        for i, n in vectors:
            entries.append([i, n, []])
        return Clock(entries, value)

    def new_list_with_history(self, vector: Vector, value) -> Clock:
        """
        Same as new_with_history, but receives a list of values, instead of a single value.
        """
        if not isinstance(value, list):
            return self.new_list_with_history(vector, [value])
        return self.new_with_history(vector, value)

    def sync(self, clock: Clock) -> Clock:
        """
        Synchronizes a list of clocks using _sync().
        It discards (causally) outdated values, while merging all causal histories.
        """
        return foldl(self._sync, 0, clock)

    def _sync(self, clock1, clock2) -> Clock:
        if not clock1:
            return clock2
        if not clock2:
            return clock1
        clock1_entires = clock1[0]
        clock1_values = clock1[1]
        clock2_entires = clock2[0]
        clock2_values = clock2[1]

        if self.less(clock1, clock2):
            values = clock2_values   # clock1 < clock2: return values2
        else:
            if self.less(clock2, clock1):
                values = clock1_values  # clock2 < clock1: return values1
            else:
                values = set(clock1_values + clock2_values)
                if values:
                    values = list(values)
                else:
                    values = []
        return [self._sync2(clock1_entires, clock2_entires), values]

    def _sync2(self, entries1, entries2) -> Entries:
        if not entries1:
            return entries2
        if not entries2:
            return entries1

        head1 = entries1[0]
        head2 = entries2[0]

        if cmp_fun(head2[0], head1[0]):
            return [head1] + self._sync2(entries1[1:], entries2)
        if cmp_fun(head1[0], head2[0]):
            return [head2] + self._sync2(entries2[1:], entries1)
        to_merge = head1 + [head2[1], head2[2]]
        return [self._merge(*to_merge)] + self._sync2(entries1[1:], entries2[1:])

    def _merge(self, the_id, counter1, values1, counter2, values2) -> list:
        """
        Returns [id(), counter(), values()]
        """
        len1 = len(values1)
        len2 = len(values2)
        if counter1 >= counter2:
            if counter1 - len1 >= counter2 - len2:
                return [the_id, counter1, values1]
            return [the_id, counter1, values1[:counter1 - counter2 + len2]]
        if counter2 - len2 >= counter1 - len1:
            return [the_id, counter2, values2]

        return [the_id, counter2, values2[:counter2 - counter1 + len1]]

    def join(self, clock) -> Vector:
        """
        Return a version vector that represents the causal history.
        """
        values = clock[0]
        result = []
        for value in values:
            if not value: continue
            result.append([value[0], value[1]])
        return result

    def create(self, clock, the_id) -> Clock:
        """
        Advances the causal history with the given id.
        The new value is the *anonymous dot* of the clock.
        The client clock SHOULD BE a direct result of new.
        """
        values = clock[1]
        if isinstance(values, list) and len(values) > 0:
            values = clock[1][0]
        return Clock(self.event(clock[0], the_id, values), [])

    def update(self, clock1, clock2, the_id) -> Clock:
        """
        Advances the causal history of the
        first clock with the given id, while synchronizing
        with the second clock, thus the new clock is
        causally newer than both clocks in the argument.
        The new value is the *anonymous dot* of the clock.
        The first clock SHOULD BE a direct result of new/2,
        which is intended to be the client clock with
        the new value in the *anonymous dot* while
        the second clock is from the local server.
        """
        # Sync both clocks without the new value
        [clock, values] = self._sync(Clock(clock1.entries, []), clock2)
        # We create a new event on the synced causal history,
        # with the id I and the new value.
        # The anonymous values that were synced still remain.
        clock_values = clock1.values
        if isinstance(clock1.values, list):
            clock_values = clock1.values[0]
        return Clock(self.event(clock, the_id, clock_values), values)

    def event(self, vector, the_id, value) -> Entries:
        if not vector:
            return [[the_id, 1, [value]]]
        if len(vector) > 0 and len(vector[0]) > 0 and vector[0][0] == the_id:
            if isinstance(value, list):
                values = value + vector[0][2]
            else:
                values = [value] + vector[0][2]
            return [[vector[0][0], vector[0][1]+1, values] + vector[1:]]
        if len(vector) > 0 and len(vector[0]) > 0:
            if isinstance(vector[0][0], list) or len(vector[0][0]) > len(the_id):
                return [[the_id, 1, [value]]] + vector
        return [vector[0]] + self.event(vector[1:], the_id, value)

    def size(self, clock) -> int:
        """
        Returns the total number of values in this clock set.
        """
        result = 0
        for entry in clock.entries:
            result += len(entry[2])
        return result + len(clock.values)

    def ids(self, clock) -> list():
        """
        Returns all the ids used in this clock set.
        """
        return [i[0] for i in clock[0]]

    def values(self, clock) -> list:
        """
        Returns all the values used in this clock set,
        including the anonymous values.
        """
        lst = []
        for entry in clock[0]:
            value = entry[2]
            if not value: continue
            lst.append(value)
        flat_list = []
        for sublist in lst:
            for item in sublist:
                flat_list.append(item)
        return clock[1] + flat_list

    def equal(self, clock1, clock2) -> bool:
        """
        Compares the equality of both clocks, regarding
        only the causal histories, thus ignoring the values.
        """
        if not isinstance(clock1, list):
            raise TypeError("clock1 should be a list")
        if not isinstance(clock2, list):
            raise TypeError("clock2 should be a list")
        if len(clock1) == 2 and len(clock2) == 2:
            return self._equal2(clock1[0], clock2[0])  # DVVSet
        return self._equal2(clock1, clock2)  # vector clocks

    def _equal2(self, vector1, vector2):
        if not vector1 and not vector2:
            return True
        if len(vector1) > 0 and len(vector1[0]) > 0 and len(vector2) > 0 and len(vector2[0]) > 0:
            if vector1[0][0] == vector2[0][0]:
                if len(vector1[0]) > 1 and len(vector2[0]) > 1 and vector1[0][1] == vector2[0][1]:
                    if len(vector1[0][2]) == len(vector2[0][2]):
                        return self._equal2(vector1[1:], vector2[1:])
        return False

    def _greater(self, vector1: Vector, vector2: Vector, strict) -> bool:
        if not vector1 and not vector2:
            return strict
        if not vector2:
            return True
        if not vector1:
            return False
        if vector1[0][0] == vector2[0][0]:
            dot_number1 = vector1[0][1]
            dot_number2 = vector2[0][1]
            if dot_number1 == dot_number2:
                return self._greater(vector1[1:], vector2[1:], strict)
            if dot_number1 > dot_number2:
                return self._greater(vector1[1:], vector2[1:], True)
            if dot_number1 < dot_number2:
                return False

        if cmp_fun(vector2[0][0], vector1[0][0]):
            return self._greater(vector1[1:], vector2, True)
        return False

    def less(self, clock1, clock2) -> bool:
        """
        Returns True if the first clock is causally older than
        the second clock, thus values on the first clock are outdated.
        Returns False otherwise.
        """
        return self._greater(clock2[0], clock1[0], False)

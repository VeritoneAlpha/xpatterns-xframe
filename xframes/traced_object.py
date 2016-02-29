"""
Base class for objects that support entry and exit tracing.
"""

import inspect
from sys import stderr


class TracedObject(object):
    entry_trace = False
    perf_count = None

    @classmethod
    def _print_stack(cls, stack, args, levels=6):
        print >>stderr, 'Enter:', stack[1][3], stack[1][1], stack[1][2]
        # print a few frames
        print >>stderr, '   ', stack[2][3], stack[2][1], stack[2][2], args
        for i in range(3, levels):
            if stack[i][3] == '<module>':
                break
            print >>stderr, '   ', stack[i][3], stack[i][1], stack[i][2]
        stderr.flush()

    @classmethod
    def _print_trace(cls, **kwargs):
        """ Explicitly call this to trace a specific function. """
        stack = inspect.stack()
        cls._print_stack(stack, kwargs, 8)

    @classmethod
    def _entry(cls, **kwargs):
        """ Trace function entry. """
        if not cls.entry_trace and not cls.perf_count:
            return
        stack = inspect.stack()
        cls._print_stack(stack, kwargs)
        if cls.perf_count is not None:
            caller = stack[1]
            my_fun = caller[3]
            if my_fun not in cls.perf_count:
                cls.perf_count[my_fun] = 0
            cls.perf_count[my_fun] += 1

    @classmethod
    def set_trace(cls, entry_trace=None):
        cls.entry_trace = cls.entry_trace if entry_trace is None else entry_trace

    @classmethod
    def set_perf_count(cls, enable=True):
        if enable:
            cls.perf_count = {}
        else:
            cls.perf_count = None

    @classmethod
    def get_perf_count(cls):
        return cls.perf_count


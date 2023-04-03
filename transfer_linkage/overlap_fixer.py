from collections import deque
from enum import Enum
from functools import total_ordering

@total_ordering
class EventClass(Enum):
    start = 0
    end = 1

    def __lt__(self, other):
        if self.__class__ is other.__class__:
            return self.value < other.value
        return NotImplemented

def clean_overlaps(chunk):
    breakable_input = transform(chunk)
    cleaned = slow_break(breakable_input)
    return inverse_transform(cleaned)

def transform(intervals):
    return sorted([(date, xid, category) 
                   for xid, *dates in intervals 
                   for date, category in zip(dates, [EventClass.start, EventClass.end])
                  ])

def inverse_transform(events):
    intervals = []
    for i in range(int(len(events)//2)):
        start = events[2*i]
        end = events[2*i+1]
        assert start[1] == end[1]
        intervals.append((start[1], start[0], end[0]))
    return intervals

def slow_break(times:list):
    times.sort()

    t, fid, state = None, None, None
    records = []
    stack = deque()

    while times:
        ti, fi, si = times.pop(0)
        # 
        if state is None and not stack:
            fid = fi
            assert si is EventClass.start
            state = si
            records.append((ti, fid, state))
        elif si is EventClass.start and fi == fid:
            records.append((ti, fid))
            records.append((ti, fi))
            stack.append(fid)
        elif si is EventClass.start and fi != fid:
            # overlapping admission. need to handle
            stack.append(fid)
            records.append((ti, fid))
            fid = fi
            records.append((ti, fi))
        elif si is EventClass.end and fi == fid:
            # done with this admission
            records.append((ti, fi))
            fid, state = None, None
            if stack:
                state = EventClass.start
                fid = stack.pop()
                records.append((ti, fid, state))
        elif si is EventClass.end and fi != fid:
            # record ends before we return to it
            stack.remove(fi)

    return records
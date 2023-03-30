from collections import deque

def clean_overlaps(chunk):
    breakable_input = transform(chunk)
    cleaned = slow_break(breakable_input)
    return inverse_transform(cleaned)

def transform(intervals):
    return sorted([(date, xid, category) for xid, *dates in intervals for date, category in zip(dates, 'se')])

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
    cont = False
    records = []
    stack = deque()

    while times:
        ti, fi, si = times.pop(0)
        # 
        if state is None and not stack:
            fid = fi
            assert si == 's'
            state = si
            records.append((ti, fid, state))
        elif si == 's' and fi == fid:
            records.append((ti, fid, 'e'))
            records.append((ti, fi, 's'))
            cont = True
        elif si == 's' and fi != fid:
            # overlapping admission. need to handle
            stack.append(fid)
            records.append((ti, fid, 'e'))
            fid = fi
            records.append((ti, fi, 's'))
        elif si == 'e' and fi == fid:
            if cont:
                cont = False
                continue
            # done with this admission
            records.append((ti, fi, si))
            fid, state = None, None
            if stack:
                state = 's'
                fid = stack.pop()
                records.append((ti, fid, state))
        elif si == 'e' and fi != fid:
            # record ends before we return to it
            stack.remove(fi)

    return records
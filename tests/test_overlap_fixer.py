from transfer_linkage import overlap_fixer


def test_simple_overlap():
    data = [
        ('0001', 1, 4,),
        ('0002', 2, 6,),
    ]

    fixed_intervals = overlap_fixer.clean_overlaps(data)

    assert fixed_intervals == [
        ('0001', 1, 2),
        ('0002', 2, 6),
    ]

def test_returning_overlap():
    data = [
        ('001', 1, 6),
        ('002', 2, 4),
    ]

    fixed_intervals = overlap_fixer.clean_overlaps(data)

    assert fixed_intervals == [
        ('001', 1, 2),
        ('002', 2, 4),
        ('001', 4, 6),
    ]

def test_multiple_admissions():
    data = [
        ('001', 1, 6),
        ('001', 7, 9),
    ]

    fixed_intervals = overlap_fixer.clean_overlaps(data)

    assert fixed_intervals == data

def test_nesting_overlaps():
    data = [
        ('a', 1, 4),
        ('b', 2, 6),
        ('c', 3, 5),
        ('b', 7, 8),
    ]

    fixed_intervals = overlap_fixer.clean_overlaps(data)

    assert fixed_intervals == [
        ('a', 1, 2),
        ('b', 2, 3),
        ('c', 3, 5),
        ('b', 5, 6),
        ('b', 7, 8),
    ]

def test_continuity_case():
    data = [
        ('a', 1, 3),
        ('a', 2, 4),
    ]

    fixed_intervals = overlap_fixer.clean_overlaps(data)

    assert fixed_intervals == [
        ('a', 1, 2),
        ('a', 2, 4),
    ]
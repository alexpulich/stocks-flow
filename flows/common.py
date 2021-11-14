def transform_number(x: str) -> float:
    mult_symb = x[-1]
    if mult_symb == 'B':
        mult = 1000000000
    elif mult_symb == 'M':
        mult = 1000000
    elif mult_symb == 'K':
        mult = 1000
    else:
        raise ValueError(f'Unsupported number {x}')
    number = float(x[:-1])
    return mult * number

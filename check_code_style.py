import subprocess

o = subprocess.check_output(['python', '-m', 'pylint', 'ring_buffer'])


def parse_grade_from_pylint_output(output: bytes) -> float:
    # result will be show at the 2 last line
    # ex: b'Your code has been rated at 10.00/10 (previous run: 10.00/10, +0.00)'
    _o = output.splitlines()[-2]
    # split by space and get the 6 element
    # ex: [b'Your', b'code', b'has', b'been', b'rated', b'at', b'10.00/10', b'(previous', b'run:', ...]
    list_output = _o.split(b' ')
    # split the result string by "/"
    # ex: '10.00/10' => ['10.00','10']
    # convert '10.00' to float
    return float(list_output[6].split(b'/')[0])


result = parse_grade_from_pylint_output(o)
if result != 10.0:
    print(f'result cannot accepted, grade = {result}')
else:
    print(f"result is accepted, grade = {result}")

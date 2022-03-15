def seq(start, stop, step=1, digit=0):
    x = float(start)
    v = []
    while x <= stop:
        v.append(round(x,digit))
        x += step
    return v
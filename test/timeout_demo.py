import time
from util.dispacher import Dispacher


def test_fun(i):
    time.sleep(4)
    a = i * i

    return a


def main_fun():
    c = Dispacher(test_fun, 20)
    c.join(3)

    if c.isAlive():
        return "TimeOutError"
    elif c.error:
        return c.error[1]
    t = c.result
    return t


if __name__ == '__main__':
    fun = main_fun()
    print(fun)

import polars as pl


trd
def ib_pl_print(object):


def ib_convert_object(object):
    r_object = vars(object)

    for o in r_object:
        if hasattr(r_object[o], '__dict__'):
            r_object[o] = ib_convert_object(r_object[o])

    return r_object


xd = ib_convert_object(trd)

xd
hasattr(vars(trd)['contract'], '__dict__')

isinstance( vars(trd)['contract'], class)
object = vars(object)







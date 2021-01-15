import functools


def decorator(func):
    @functools.wraps(func)
    def inner(ref, *args, **kwargs):
        print("inside decorator!")
        print(ref)
        ref.load_cfg()
        print("updated config!")
        func(ref, *args, **kwargs)
    return inner

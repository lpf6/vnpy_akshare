import functools
import inspect


def except_method(reset_func=None, try_count=3, *ds, **kwds):
    """
    给类成员函数使用的注解方法
    :param reset_func: 重设函数
    :param ds: reset_func需要的默认参数
    :param kwds: reset_func需要的默认命名参数
    :return:
    """

    def get_wrapper(user_function):
        @functools.wraps(user_function)
        def wrapper_self(self, *args, **kwargs):
            ex = None
            result = None
            count = 0

            while count < try_count:
                try:
                    result = user_function(self, *args, **kwargs)
                    ex = None
                    break
                except Exception as ex1:
                    count += 1
                    if reset_func:
                        reset_func_sig = inspect.signature(reset_func)
                        if 'self' in reset_func_sig.parameters:
                            reset_func(self, *ds, **kwds)
                        else:
                            reset_func(*ds, **kwds)
                    else:
                        reset = getattr(self, "reset", None)
                        if callable(reset):
                            reset_func_sig = inspect.signature(reset)
                            if 'self' in reset_func_sig.parameters:
                                self.reset(*ds, **kwds)
                            else:
                                reset(*ds, **kwds)
                    ex = ex1
            if ex is not None:
                raise ex
            return result

        @functools.wraps(user_function)
        def wrapper(*args, **kwargs):

            ex = None
            result = None
            count = 0

            while count < try_count:
                try:
                    result = user_function(*args, **kwargs)
                    ex = None
                    break
                except Exception as ex1:
                    count += 1
                    if reset_func:
                        reset_func(*ds, **kwds)
                    ex = ex1
            if ex is not None:
                raise ex
            return result

        sig = inspect.signature(user_function)
        if 'self' in sig.parameters:
            return wrapper_self
        else:
            return wrapper

    return get_wrapper

class classproperty:
    """Decorator for class-level properties (read-only)"""
    
    def __init__(self, func=None, *, cached=False):
        self.func = func
        self.__doc__ = func.__doc__
        self.cached = cached
        self._cache = None
    
    def __get__(self, obj, objtype=None):
        if self.cached and self._cache:
            return self._cache
        if objtype is None:
            # The objtype is the class definition, and this guard protects against
            # the abnormal edge case where Python doesn't pass an objtype.
            objtype = type(obj)
        result = self.func(objtype)
        self._cache = result
        return result
    
    def __set__(self, obj, value):
        raise AttributeError("can't set attribute")
    
    def __call__(self, func):
        # Support decorator usage with parameters: @classproperty(...)
        self.func = func
        return self
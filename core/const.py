"""
    Named constants for the CDR Scheduler.

    This is very clever and requires some explanation.  It comes
    from http://stackoverflow.com/a/2688086/282194 which is adapted
    from the examples at https://docs.python.org/2/library/functions.html#property

    The constant function is used with a decorator to create the
    illusion of a named constant.  There are three pieces to it:

    1.  The fset() function. This function is called when there is an
        attempt to assign a value to the constant. Instead of changing the
        value, fset() raises TypeError.

    2.  The fget() function. This function is called when there is an
        attempt to read the constant's value. It simply calls the 

    3.  The returned property attribute. This is what hooks up the getter and
        setter functions.

    To create a set of constant values:

    1.  Create a class to contain the values.
    2.  For each name, create a method that returns the constant value.
    3.  Decorate the methods with @constant.
    4.  Create an identifier for refererncing the values outside the module.

        class _MyConstants:
            @constant
            def PI():
                return 3.1415
            @constant
            def FOO():
                return 12345

        MyConstants = _MyConstants()

    When one of the name methods is referenced, the decorator causes the getter
    or setter to be invoked instead.

        # This outputs 3.1415
        print MyConstants.PI

        # This raises a TypeError
        MyConstants.PI = 5

"""

def constant(f):
    """
    Decorator function to create getter and setter functions for
    any decorated property.
    """
    def fset(self, value):
        "Setter function. Throws an exception to prevent the 'constant' from being overwritten."
        raise TypeError("Attempt to set a read-only value.")
    def fget(self):
        "Getter function. Invokes the decorated function to return its value."
        return f()
    return property(fget, fset)


class _TaskStatus(object):
    @constant
    def OK():
        'Normal status. Everything is good.'
        return 0
    @constant
    def ERROR():
        'A task encountered an error, but was able to continue. (e.g. an invalid database record)'
        return 1
    @constant
    def FATAL():
        'A task encountered an error and was unable to continue. (e.g. the database was unavailable.)'
        return 1

TaskStatus = _TaskStatus()

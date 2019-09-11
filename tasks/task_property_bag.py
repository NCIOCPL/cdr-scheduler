from core.const import TaskStatus

class TaskPropertyBag(object):
    """
        Data structure for passing information between successive tasks in a CDRJob.
        
        properties - (internal) dictionary containing named data items.
            Status - (internal) flag containing a TaskStatus structure.
    """

    def __init__(self):
        self.status = TaskStatus.OK
        self.properties = {}

    def Get(self, name, default):
        "Retrieves a named value."
        value = self.properties.get(name, default)
        return value
    
    def Set(self, name, value):
        "Sets a named value."
        self.properties[name] = value
        pass
        
    def GetStatus(self):
        return self.status

if __name__ == "__main__":
    pb = TaskPropertyBag()
    print('Status is OK: %s' % (pb.GetStatus() == TaskStatus.OK, ))
    
    # Test for value that should exist.
    pb.Set('prop1', 5)
    res = pb.Get('prop1', 'not found')
    if( res != 5 ):
        print('prop1 fail: expected 5, found \'%s\'.' % (res,))
    else:
        print('Success for existing value.')
        
    # Test for value that should not exist.
    res = pb.Get('prop2', 25)
    print('OK for value that shouldn\'t exist: %s' % (res == 25))
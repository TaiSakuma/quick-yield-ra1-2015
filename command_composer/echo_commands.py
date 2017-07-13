# Tai Sakuma <tai.sakuma@cern.ch>

##__________________________________________________________________||
class EchoCommands(object):
    def __init__(self, commands):
        self.commands = commands

    def __call__(self):
        return self.commands

##__________________________________________________________________||

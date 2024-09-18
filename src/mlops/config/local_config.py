import uuid


class MockDBUtils:
    def __init__(self):
        self._notebook = MockNotebookUtils()

    def notebook(self):
        return self._notebook


class MockNotebookUtils:
    def getContext(self):
        return MockContext()


class MockContext:
    def jobId(self):
        return MockOptional(str(uuid.uuid4()))

    def jobName(self):
        return MockOptional("")


class MockOptional:
    def __init__(self, value):
        self._value = value

    def get(self):
        return self._value

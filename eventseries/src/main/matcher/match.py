class Match:
    def __init__(self, event, event_series) -> None:
        self._event = event
        self._event_series = event_series

    def get_event(self):
        return self._event

    def set_event(self, event):
        self._event = event

    def get_event_series(self):
        return self._event_series

    def set_event_series(self, event_series):
        self._event_series = event_series

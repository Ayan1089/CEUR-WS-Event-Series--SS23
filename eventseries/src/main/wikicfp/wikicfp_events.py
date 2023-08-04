import pandas as pd


def series_cfp_input():
    df_series_wikicfp = pd.read_json("../resources/eventserise_wikicfp.json")
    return df_series_wikicfp


def event_cfp_input():
    df_event_wikicfp = pd.read_json(
        "../resources/event_cfp_with_Time_Acronym_Title.json"
    )
    return df_event_wikicfp


def all_series_cfp_obj():
    df = series_cfp_input()
    df_l = []
    for index, row in df.iterrows():
        dic = {
            "index": index,
            "series": series_cfp(
                ID=row["wikiCfpId"], title=row["title"], dblp=row["dblpSeriesId"]
            ),
        }
        df_l.append(dic)
    return df_l


class series_cfp:
    def __init__(self, ID, title=None, dblp=None):
        self.ID = ID
        self.title = title
        self.dblp = dblp

    # 'dblpSeriesId', 'deleted', 'seriesId', 'title', 'url', 'wikiCfpId',
    # 'source', 'eventSeriesId', 'acronym'],

    def get_dblp(self):
        if self.dblp == None:
            df = series_cfp_input()
            self.dblp = df[df["seriesId"] == self.ID]["dblpSeriesId"].values
            return self.dblp

        else:
            return self.dblp

    def get_Title(self):
        if self.title == None:
            df = series_cfp_input()
            self.title = df[df["seriesId"] == self.ID]["title"].values
            return self.title

        else:
            return self.title

    def get_acronym(self):
        df = series_cfp_input()
        self.acronym = df[df["seriesId"] == self.ID]["acronym"].values
        return self.acronym

    def get_url(self):
        df = series_cfp_input()
        self.url = df[df["seriesId"] == self.ID]["url"].values
        return self.url

    def get_relativ_events(self):
        df = event_cfp_input()
        events_l = []
        df_event = df[df["seriesId"] == self.ID]
        for index, row in df_event.iterrows():
            events_l.append(
                event_cfp(ID=row["eventId"], title=row["title"], year=row["year"])
            )

        self.events = events_l
        return self.events


class event_cfp:
    def __init__(
        self, ID, title=None, year=None, seriesID=None, seriesTitle=None, location=None
    ):
        self.ID = ID
        self.title = title
        self.year = year
        self.seriesID = seriesID
        self.seriesTitlt = seriesTitle
        self.location = location

    def get_location(self):
        df = event_cfp_input()
        self.location = df[df["eventId"] == self.ID]["locality"].values
        return self.location

    # 'Submission_Deadline', 'acronym', 'deleted', 'endDate', 'eventId',
    #   'eventType', 'locality', 'series', 'seriesId', 'startDate', 'title',
    #   'wikiCfpId', 'year', 'source', 'url', 'ordinal', 'city',
    #   'cityWikidataid', 'region', 'regionIso', 'regionWikidataid', 'country',
    #   'countryIso', 'countryWikidataid', 'lookupAcronym', 'Notification_Due',
    #   'Final_Version_Due'
    def get_acronym(self):
        df = event_cfp_input()
        self.acronym = df[df["eventId"] == self.ID]["acronym"].values
        return self.acronym

    def get_eventType(self):
        df = event_cfp_input()
        self.eventType = df[df["eventId"] == self.ID]["eventType"].values
        return self.eventType

    def get_url(self):
        df = event_cfp_input()
        self.url = df[df["eventId"] == self.ID]["url"].values
        return self.url

    def get_Title(self):
        if self.title == None:
            df = event_cfp_input()
            self.title = df[df["eventId"] == self.ID]["title"].values
            return self.title

        else:
            return self.title

    def get_attr(self, attr):
        df = event_cfp_input()
        return df[df["eventId"] == self.ID][attr].values

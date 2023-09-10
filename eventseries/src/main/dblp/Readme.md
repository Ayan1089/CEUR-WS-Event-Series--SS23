# Scrape and parse information about events and their series form dblp.org

### Overview

- `DblpContext.py` contains the logic for requesting pages and caching them.
- `DblpScraper.py` contains the logic for navigating dblp.org and requesting the correct pages.
- `DblpParser.py` is used to extract the informations from the requested html-files
- The parsed information is structured in dataclasses defined in the files `VenueInformationClasses.py`
  and `EventClasses.py`

### How dblp.org stores information

#### EventSeries

- The `name` is found in a `h1` tag inside the unique `header` tag with the `id=headline`
    - The abbreviation is within the name in parenthesis
- The venue information is given as `li` tags inside the unique div with the `id=info-section`
- The events are not in a particular structured form
    - They are `header` tags within the `div` with the `id=main`
    - Watch out to not include recursive search for finding `header'

#### Events

- The year, location, ordinal and title are all found in a `h2` tag
- The usual form is `[ordinal] title [year] [:location]`
    - However there are quite a lot of irregularities listed below
    - Sometimes one `h2` tag refers to multiple events seperated with `/`
        - `[ordinal] title (/ [ordinal title)* [year] [:location]`
        - However this is also [not consistent](https://dblp.org/db/conf/wlp/index.html)

### Mapping events to event-series

#### Standard Cases
- Use the *breadcrumb* section of the event to extract the *dblp id* of the event series
  - This is clearly defined only for conferences
  - For workshops one has to distinguish between multiple linked series
    - For example the [iStar](https://dblp.org/db/conf/istar/istar2015.html) workshop has two links
  - Or extract workshop series information from the linked conference series
    - For example the workshop [SocInf](https://dblp.org/db/conf/ijcai/socinf2015.html) only links to the conference series [IJCAI](https://dblp.org/db/conf/ijcai/index.html) which contains a list of workshop series
    - This information can be read from the `VenueInformation` of the parsed series 
- There is an additional workshop page for one conference
  - Usually the id will be the same as the conference with an additional "w" suffix
  - e.g. [conf/iccbr/iccbr2015](https://dblp.org/db/conf/iccbr/iccbr2015.html)
    - With [conf/iccbr/iccbr2015w](https://dblp.org/db/conf/iccbr/iccbr2015w.html) being the list of workshops for this conference

#### Special Cases
- [Workshop](https://dblp.org/db/conf/birws/birws2021.html) has two series listed in breadcrumbs
    - The name of one is BIR but the dblp-id links to ["conf/birws"](https://dblp.org/db/conf/birws/index.html)
        - ["conf/bir"](https://dblp.org/db/conf/bir/index.html) would lead to the conference series
    - The title contains the "@" pattern "BIR@ECIR"
    - Following the id constructed form the "@" pattern would lead to the wrong series

### Known Issues

- Meta "event series"
    - [Birthday](https://dblp.org/db/conf/birthday)
    - [Advanced Courses](https://dblp.org/db/conf/ac/index.html)
- Non Events
    - Non events appended at end [FM](https://dblp.org/db/conf/fm/index.html)
    - [Book: 20 Years](https://dblp.org/db/conf/clef/index.html)
    - [Case-Based Reasoning Technology, 1998](https://dblp.org/db/conf/iccbr/index.html)
- ; used between year and location
    - [NESY](https://dblp.org/db/conf/nesy/index.html)
    - [DL 1992; Boston, MA, USA](https://dblp.org/db/conf/dlog/index.html)
- Bad formatting for year
    - [4th 1980 Amsterdam](https://dblp.org/db/conf/ecai/index.html)
        - Also misses the name
    - [IR 1995 Workshop: San Francisco, California, USA](https://dblp.org/db/conf/irep/index.html)
- Bad location formatting
    - [33rd DL 2020: Online Event [Rhodes, Greece]](https://dblp.org/db/conf/dlog/index.html)
    - [virtual] as location, no : in between
        - conf/staf
        - conf/nips
        - conf/nips
        - conf/fm
        - conf/se
        - conf/aiide
        - conf/aiide
        - conf/hcomp
        - conf/hcomp
- Extra formatting for multiple events
    - [DECLARE 2017 (21st INAP 2017 / 25th WFLP 2017 / 31st WLP 2017): Würzburg, Germany](https://dblp.org/db/conf/wlp/index.html)
- Missing : between year and location
    - [9th CIKM 2000 Washington, DC, USA](https://dblp.org/db/conf/cikm/index.html)
    - [CIR 2000 Brighton, UK](https://dblp.org/db/conf/civr/index.html)
    - [CIR 1999 Newcastle, UK](https://dblp.org/db/conf/civr/index.html)
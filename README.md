## IC3 Annual reports (2016-2022)

### Overview:
This solution extracts 5 table data provided at the [official IC3 webpage](https://www.ic3.gov/Media/PDF/AnnualReport/2016State/StateReport.aspx#?s=1) for 57 states (1-57) and saves it to a data folder, partitioned by year/state.

### How to use:
1. Clone repository to your location.
2. activate virtual environment. [Documentation on Howto](https://python.land/virtual-environments/virtualenv)
3. open terminal and install requirements.txt  
`pip install -r requirements.txt`

#### To load/update all data: 
in terminal write :
`python3 main.py`

#### To extract specific data for different years/states:
1. open your python terminal. in terminal write:
`python3`
2. import ProcessOrchestrator  
`from src.orchestrator import ProcessOrchestrator`
3. Initiate process ProcessOrchestrator class providing required data: 
    * a set of state indexes for data load. Example: `{1,12,44}` 
    * a set of years for data load. Example: `{2016}`
    
    **Note:** that state & index values can be found in documentation folder.

    to initiate data extraction for the values presented above:  
    `ProcessOrchestrator(index_range={1,12,44}, year_range={2016}).get_all_data()`


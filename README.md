## Urban Pedestrian Mobility as a Renewable Energy Resource
### Repository structure
```
│   .gitignore
│   README.md
│   requirements.txt
│   
├───assets (contains any pdfs, images, etc).
├───data
│      │
│      └───france (OSM dataset)
│       
│       
├───lib
│       load_netmob.py
│       schema.py
│       
└───notebooks
        case_study_areas.ipynb
        feature engineering.ipynb
        merge_gps_roads.ipynb
        modeling.ipynb
        Preliminary EDA.ipynb
        raw_trip_blurring_removed.ipynb
        rush_hours.ipynb
```
### Dependencies
1. Request access to the NetMob 2025 Data Challenge dataset from:
https://netmob.org/www25/datachallenge#form
2. Download the Île-de-France OSM extract from:
https://download.geofabrik.de/europe/france/ile-de-france.html
Place the downloaded files inside:

./data/france/

3. Install all required Python packages::
```
pip install -r requirements.txt
```
4. This project was developed and tested using Python 3.11.7 and PySpark 4.0.0. Other versions may work but have not been officially tested.

### How to run on your device ?
#### GPS-to-Road Mapping
To associate GPS points from the NetMob dataset with the nearest roads from OpenStreetMap (OSM), we performed spatial joins between GPS trajectories and the road network.

Since spatial joins are computationally expensive, the processing was performed in batches, where each batch contains 10 files.

To reproduce this step:

Open notebooks/merge_gps_roads.ipynb.
1. Run all notebook cells.
2. The notebook will generate:
3. `data/gps_road_merged_v2.csv`

Processing may take a considerable amount of time depending on the available hardware resources.

#### `Load_NetMob` Class
The `Load_NetMob` class simplifies dataset loading, merging, and preprocessing.

Parameters

datasets (list, default=[1]):
GPS data serves as the base dataset, while the selected datasets are merged onto it.

Available options:
1. Trips dataset
2. Individuals dataset
3. Weather dataset

modes (list, default=['WALKING']):

Transportation modes to include in the returned dataset.

Example
```python
from load_netmob import Load_NetMob

# Load GPS, trips, individuals, and weather data
df = Load_NetMob([1, 2, 3])
```

#### Selection of Case Study Regions and Streets
The methodology used to identify the study regions and streets can be found in: `/notebooks/case_study_areas.ipynb`.

#### Comparison of Transportation Modes
Exploratory analysis comparing the main transportation modes is available in: `/notebooks/Preliminary EDA.ipynb`.

#### Peak Pedestrian Activity Analysis
Analysis of peak pedestrian activity periods for each study region can be found in: `/notebooks/rush_hours.ipynb`.

### Evaluation of the Proposed Deblurring Technique
The proposed deblurring technique was evaluated on multiple trips and visualized using Kepler.gl by comparing the original and deblurred trajectories.

The implementation is available in:

`notebooks/raw_trip_blurring_removed.ipynb`

To evaluate a different trip, modify the `KEY` variable in the notebook.

### Road Proximity Filtering
A feature named `distance_to_road` was generated using GeoPandas to measure the distance between each GPS point and its nearest road segment.

The implementation can be found in:

`notebooks/merge_gps_roads.ipynb`

These distances were subsequently used to filter GPS points and trips that were farther than 10 meters from their nearest road. Different threshold values were evaluated and visualized using Kepler.gl.

### Feature Engineering
Feature engineering steps used in the study are provided in: `notebooks/feature engineering.ipynb`.

### Modeling
Model training, evaluation, and experimental results are provided in: `notebooks/modeling.ipynb`.
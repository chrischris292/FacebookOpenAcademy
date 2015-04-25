## OpenNLP Text Category Classifier Engine Template

This engine template has integrated OpenNLP's GISModel for text classification.

### Overview
This engine template utilizes the GIS algorithm from the Apache OpenNLP library to classify text based off of training data. 

## Versions

### v0.1.0

- initial version

### Usage
**Event Data Requirements**

**Input Query**
* Uses description and message to classify
* Category

**Output PredictedResult**
* Category


### 1. Run PredictionIO

If PredictionIO is not installed, install it [here](http://docs.prediction.io/install/).

Start all components (Event Server, Elaticsearch, and HBase).

Note: If `pio-start-all` is not recognized, upgrade to the latest version of PredictionIO.
```
$ pio-start-all
```

Verify the status of components:
```
$ pio status
```

### 3. Create a new application
```
$ pio app new [YourAppName]
```

The console output should include the App Name, **App ID**, and **Access Key**. You will need the App ID and Access Key in future steps. You can view your applications by entering `pio app list`.

### 4. Import Data to the Event Server

Install the PredictionIO Python SDK:
```
$ pip install predictionio
```
or
```
$ easy_install predictionio
```

From the root directory of your engine, run:
```
$ python data/import_eventserver.py --access_key [YourAccessKeyFromStep3] --file [/path/to/your/data]
```

### 5. Build, Train, and Deploy the Engine

From the root directory of your engine, find `engine.json` and verify that the appId matches the **App Id** of your application from Step 3.

```
 ...
  "datasource": {
    "params" : {
      "appId": App id from step 3 here
    }
  },
  ...
```

Build the engine.
```
$ pio build
```

Train the engine. This may take several minutes.
```
$ pio train
```

Deploy the engine. This may take several minutes.
```
$ pio deploy
```

After deploying successfully, you can view the status of your engine at [http://localhost:8000](http://localhost:8000).

### 6. Using the Engine
To do a sample query, run `python send_data.py` from the root directory of your engine. Customize the query by modifying the JSON `"sentence" : "Want your skin feeling softer, fuller?"` in `send_data.py`. 
# Datex Azure
##Traffic related data analysis using Datex2 and Microsoft Azure.
The project's aims to gather and process online traffic information continuously published by the Norwegian road authorities (Vegvesenet) in accordance to a European standard Datex2. The project is hosted at Microsoft Azure.

##Information about Datex2
The introduction to Datex2 protocol can be found [here](http://www.vegvesen.no/en/Traffic/On+the+road/Datex2/How+to+get+started).

##Data sources
###Weatherdata publications:
https://www.vegvesen.no/ws/datex/get/1/GetMeasurementWeatherSiteTable
https://www.vegvesen.no/ws/datex/get/1/GetMeasuredWeatherData 
###WEB-camera  publication:
https://www.vegvesen.no/ws/datex/get/1/GetCCTVSiteTable
###Traveltime publications:
https://www.vegvesen.no/ws/datex/get/1/GetPredefinedTravelTimeLocations
https://www.vegvesen.no/ws/datex/get/1/GetTravelTimeData 
###Situation publication:
https://www.vegvesen.no/ws/datex/get/1/GetSituation

Access to Vegvesenet services requires username and password (available at request for project participants).

##Projected functionality
The main goal of the Datex Azure is to accumulate historical measurements from Vegvesenet's Datex services, so they can be used as a playground to try various data analysis and machine learning algorithms. Since historical data is not part of Datex2 protocol or Vegvesenet's services, the very first project's task is to set up continuous data gathering. This is implemented using Azure WebJob that polls Vegvesenet's services and stores responses in Azure blobs.

Next task is to build traffic event stream from Datex2 data sets. Datex2 services always return snapshots of the current traffic situation, so the blobs stored from the polling WebJob contain mostly overlapping information. It's planned to create another WebJob that will normalize incoming Datex2 data and build event streams.

Once Datex2 event streams are in place, they can be used for data mining, machine learning or any other suitable application. 
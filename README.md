
	Engine database: Mongodb 4.0
	Essential libraries:
		pymongo 	3.7.1
		dask		0.19.0
		sqlAlchemy	1.2.11
		panda		0.23.4
	
	configuration:
		1- define organizations
		2- define organization business entities
		3- define organization attributes
		4- define organization source connections
		5- define business entity attributs
		6- define business entity sources
		7- mapping
		8- define execution plan
	workflow:
		1- extract data from source system
		2- load it to staging area
		3- melt data, to be in long formate (variable and value) instead of wide formate
		4- compare the the melted data with the existing
		5- extract difference 
		6- move old data to archive table
		7- push new/changed data to current active table
		8- apply rules on each data value based on pre configred execution plane
		9- generate result in new table with data quality issues for each business entities


# Eviction-Compass
The Eviction Tracker is a data analytics platform that helps with real-time eviction pattern analysis used in the interpretation of the entire state of California.

 
Housing instability and evictions are current dilemmas in urban areas, which in general, reflect underlying socio-economic risks. San Francisco, due to its high cost of living and its dynamic housing market, has attracted considerable interest around the topic of eviction data and the implications it has for public policy and community well-being. In an effort to help data-informed decision-making to meet the challenges, the dissemination and analysis of open eviction data has become a major concern. The goal of this project is to use the current data warehousing and visualization tools and knowledge in order to derive practical and actionable knowledge from eviction data collected from the San Francisco Government Open Data Portal.
 
The main goal of this work is to develop and implement a complete data pipeline and analysis framework able to automate data ingestion, processing, storage, and visualization. By building a scalable and efficient architecture, the system ensures that eviction data can be systematically analyzed for trends, anomalies, and correlations, offering valuable insights to stakeholders, including policymakers, researchers, and community organizations.
 
For this, we repackaged some of the state-of-the-art tools and technologies into the workflow
 
Data Collection and Storage: The eviction information is retrieved through an API of SF Gov Open Data Portal and is stored securely in Amazon S3. This guarantees a robust and scalable repository for the unprocessed data.
 
Automation with Apache Airflow: Task orchestration and process automation are managed using Apache Airflow, which schedules and monitors the extraction and loading of data.
 
Data Warehousing with Snowflake: Snowflake is used to build scalable structured dimension and fact tables, enabling fast querying and aggregation of data. In this stage unprocessed data are transformed into a format suitable for analytical processing.
 
Data Visualization with MongoDB Atlas: Most important insights are represented by using MongoDB Atlas, providing eviction patterns and trends in a user-friendly and interactive mode for end users.
 
Not only this project illustrates the capability of combining cloud services and automating tools but also highlight the significance of open data in solving social challenges. Through the transduction of raw eviction data into actionable knowledge, the system not only creates a better insight of the trends underlying them, but also provides a basis for evidence-based interventions.

# Data Engineering Project

### Filtering Covid-19 Data from various data sources listed in HealthData and automating the workflow using Apache Airflow

### Tools used
<ol>
  <li> Python  </li>
  <li> Docker </li>
  <li> Airflow </li>
  <li> PostgresSQL </li>
  <li>Plotly to visualize the data</li>
 </ol>
  

The main idea is to bring in daily data for COVID-19 for all the states and store the information in PostgresSQL.

The data is collected from the website,
Website: https://healthdata.gov/data.json

#### Steps:
<ol>
<li> Parse through the HeathData API, data.json file and filter COVID related titles. 
</li>
<li> Downloading all the filtered data and performing validations and normalizations, to the dataset.
</li>
<li> After normalizing the dataset, I have stored the relavent datafiles in respective folders.
</li>
<li> Created a schema named "covid" and created all the files with respective columns.
</li>
<li> Inserted all the data and dependencies into the postgres dataset.
</li>
</ol>

The flow of the data is as follows.

![workflow](https://github.com/dhanasettyabhishek/COVID-19_workflow/blob/master/config/other/workflow.png?raw=true)


#### Key factors:
<ol>
<li>The workflow can be scaled to large amounts of data</li>
<li>Parallelized the workflow, based on the task status</li>
<li>Logging the information if any failure occurs.</li>
<li>Monitored and used Object-Oriented Programming in Python</li>
<li>Used Second Normal Form, for designing a schema.</li>
<li>On average, it takes 7 seconds to complete the entire process.</li>
</ol>
Duration:

![duration](https://github.com/dhanasettyabhishek/COVID-19_workflow/blob/master/config/other/duration.png?raw=true)


#### Execution

<ul>
<li>Clone the repository:</li>

https://github.com/dhanasettyabhishek/COVID-19_workflow.git
<li>Using Docker compose, simply run the command docker-compose up</li>
<li>Visit localhost:8080 to visit airflow.</li>

http://localhost:8080/admin/
</ul>

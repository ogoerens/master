
# Secure Database Performance
## Introduction
Repository containing the code for the framework used in the master thesis "Secure Database Performance". Link to Masterthesis: [https://doi.org/10.3929/ethz-b-000614214](https://doi.org/10.3929/ethz-b-000614214).
## Install 
The application is built using Maven. To install, follow these steps:

1.  Clone the repository to your local machine using `git clone https://github.com/ogoerens/master.git`.
2.  Navigate to the `Master` directory with `cd master/Master`.
3.  Run `mvn clean install` to build and install the application.

## Usage
The framework provides several command line options:


|     Input      |Description|
|----------------|-------------------------------|
|-g filename|`generate Datasets `            |
|-c filename|`connect to a DB `            |
|-dm filename          |`Creates/Updates tables in the DB `|
|-a filename|`Anonymize dataset/queries` |
|-q filename| `Specify a personalized queryset. Queryset name is "psersonalized"`|
|-e queryset_name|`Execute the queries of the specified set. Multiple sets at once possible` |
|-numExecutions x|`Change the number of executions for each query`|

Example files can be found [here](https://github.com/ogoerens/master/tree/main/Master/src/main/resources).


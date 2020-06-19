# Sieve 

![Sieve Logo](images/logo.png)

SIEVE is a general purpose middleware to support access control in DBMS that enables them to scale query processing with very large number of access control policies. Full version of the paper can be seen at [arXiv](https://arxiv.org/abs/2004.07498). 


## Setup

1. Create database in either MySQL or PostgreSQL
2. Load schema and data inside the data directory (wifi for MySQL and mall for PostgreSQL)
3. Update the sample.properties file inside resource/credentials directory

## Usage

1. Set the dbms and table_name options in resource/general.properties
2. Set true for the experiments that you wish to run (Options: Query Performance, Policy Scale up)
3. Compile the code
```
mvn clean install

```
4. Execute it with
```
mvn exec:java 
```

## License
[Apache 2.0](https://choosealicense.com/licenses/apache-2.0/)


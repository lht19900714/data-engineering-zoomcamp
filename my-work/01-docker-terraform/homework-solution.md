## Module 1 Homework

## Docker & SQL

In this homework we'll prepare the environment 
and practice with Docker and SQL


## Question 1. Knowing docker tags

Run the command to get information on Docker 

```docker --help```

Now run the command to get help on the "docker build" command:

```docker build --help```

Do the same for "docker run".

Which tag has the following text? - *Automatically remove the container when it exits* 

- `--delete`
- `--rc`
- `--rmc`
- `--rm`

## Question 1 Answer: `--rm`


## Question 2. Understanding docker first run 

Run docker with the python:3.9 image in an interactive mode and the entrypoint of bash.
Now check the python modules that are installed ( use ```pip list``` ). 

What is version of the package *wheel* ?

- 0.42.0
- 1.0.0
- 23.0.1
- 58.1.0


## Question 2 Answer: 23.0.1

# Prepare Postgres

Run Postgres and load data as shown in the videos
We'll use the green taxi trips from September 2019:

```wget https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2019-09.csv.gz```

You will also need the dataset with zones:

```wget https://s3.amazonaws.com/nyc-tlc/misc/taxi+_zone_lookup.csv```

Download this data and put it into Postgres (with jupyter notebooks or with a pipeline)


## Question 3. Count records 

How many taxi trips were totally made on September 18th 2019?

Tip: started and finished on 2019-09-18. 

Remember that `lpep_pickup_datetime` and `lpep_dropoff_datetime` columns are in the format timestamp (date and hour+min+sec) and not in date.

- 15767
- 15612
- 15859
- 89009

## Question 3 Answer: 15612
select count(*) from green_taxi_data  
where (lpep_pickup_datetime >= '2019-09-18' and lpep_pickup_datetime < '2019-09-19')  
and (lpep_dropoff_datetime >= '2019-09-18' and lpep_dropoff_datetime < '2019-09-19')


## Question 4. Largest trip for each day

Which was the pick up day with the largest trip distance
Use the pick up time for your calculations.

- 2019-09-18
- 2019-09-16
- 2019-09-26
- 2019-09-21

## Question 4 Answer: 2019-09-26

SELECT lpep_pickup_datetime::date AS pickup_day, MAX(trip_distance) AS max_distance  
FROM green_taxi_data  
GROUP BY pickup_day  
ORDER BY 2 DESC;

SELECT to_date(to_char(lpep_pickup_datetime, 'YYYY-MM-DD'),'YYYY-MM-DD') as pickup_day, MAX(trip_distance) as max_distance
FROM green_taxi_data  
GROUP BY pickup_day  
ORDER BY 2 DESC;



## Question 5. Three biggest pick up Boroughs

Consider lpep_pickup_datetime in '2019-09-18' and ignoring Borough has Unknown

Which were the 3 pick up Boroughs that had a sum of total_amount superior to 50000?
 
- "Brooklyn" "Manhattan" "Queens"
- "Bronx" "Brooklyn" "Manhattan"
- "Bronx" "Manhattan" "Queens" 
- "Brooklyn" "Queens" "Staten Island"

## Question 5 Answer: "Brooklyn" "Manhattan" "Queens"


SELECT z."Borough", SUM(total_amount) AS total_amount  
FROM green_taxi_data AS g JOIN zones AS z ON g."PULocationID" = z."LocationID"  
WHERE lpep_pickup_datetime >= '2019-09-18'  
  AND lpep_pickup_datetime < '2019-09-19'  
  AND z."Borough" != 'Unknown'  
GROUP BY z."Borough"  
ORDER BY 2 DESC  




## Question 6. Largest tip

For the passengers picked up in September 2019 in the zone name Astoria which was the drop off zone that had the largest tip?
We want the name of the zone, not the id.

Note: it's not a typo, it's `tip` , not `trip`

- Central Park
- Jamaica
- JFK Airport
- Long Island City/Queens Plaza

## Question 6 Answer: JFK Airport

SELECT z2."Zone", MAX(tip_amount) AS max_tip  
FROM green_taxi_data AS g JOIN zones AS z1 ON g."PULocationID" = z1."LocationID"  
  JOIN zones AS z2 ON g."DOLocationID" = z2."LocationID"  
WHERE lpep_pickup_datetime >= '2019-09-01'  
  AND lpep_pickup_datetime < '2019-10-01'  
  AND z1."Zone" = 'Astoria'  
GROUP BY z2."Zone"  
ORDER BY 2 DESC  

## Terraform

In this section homework we'll prepare the environment by creating resources in GCP with Terraform.

In your VM on GCP/Laptop/GitHub Codespace install Terraform. 
Copy the files from the course repo
[here](https://github.com/DataTalksClub/data-engineering-zoomcamp/tree/main/01-docker-terraform/1_terraform_gcp/terraform) to your VM/Laptop/GitHub Codespace.

Modify the files as necessary to create a GCP Bucket and Big Query Dataset.


## Question 7. Creating Resources

After updating the main.tf and variable.tf files run:

```
terraform apply
```

Paste the output of this command into the homework submission form.

```text
Terraform used the selected providers to generate the following execution plan. Resource actions are indicated with the following symbols:
  + create

Terraform will perform the following actions:

  # google_bigquery_dataset.practice-dataset will be created
  + resource "google_bigquery_dataset" "practice-dataset" {
      + creation_time              = (known after apply)
      + dataset_id                 = "manifest-altar-412117-practice-dataset"
      + delete_contents_on_destroy = false
      + etag                       = (known after apply)
      + id                         = (known after apply)
      + labels                     = (known after apply)
      + last_modified_time         = (known after apply)
      + location                   = "US"
      + project                    = (known after apply)
      + self_link                  = (known after apply)
    }

  # google_storage_bucket.practice-bucket will be created
  + resource "google_storage_bucket" "practice-bucket" {
      + force_destroy               = true
      + id                          = (known after apply)
      + location                    = "US"
      + name                        = "manifest-altar-412117-practice-bucket"
      + project                     = (known after apply)
      + public_access_prevention    = (known after apply)
      + self_link                   = (known after apply)
      + storage_class               = "STANDARD"
      + uniform_bucket_level_access = true
      + url                         = (known after apply)

      + lifecycle_rule {
          + action {
              + type = "Delete"
            }
          + condition {
              + age                   = 3
              + matches_prefix        = []
              + matches_storage_class = []
              + matches_suffix        = []
              + with_state            = (known after apply)
            }
        }

      + versioning {
          + enabled = true
        }
    }

Plan: 2 to add, 0 to change, 0 to destroy.

Do you want to perform these actions?
  Terraform will perform the actions described above.
  Only 'yes' will be accepted to approve.

  Enter a value: 
```

## Submitting the solutions

* Form for submitting: https://courses.datatalks.club/de-zoomcamp-2024/homework/hw01
* You can submit your homework multiple times. In this case, only the last submission will be used. 

Deadline: 29 January, 23:00 CET

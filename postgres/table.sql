CREATE TABLE data (
  id varchar(255),
  start_timestamp TIMESTAMP(3) with TIME ZONE,
  end_timestamp TIMESTAMP(3) with TIME ZONE,
  num_observations_in_window INTEGER,
  text varchar(255),
  polarity_v float,
  polarity varchar(255) ,
  subjectivity_v float,
  PRIMARY KEY (id,start_timestamp)
 );
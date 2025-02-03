CREATE TABLE domains (
  id SERIAL PRIMARY KEY,
  name VARCHAR(20),
  type VARCHAR(20),
  organisation VARCHAR(200),
  dateOfEntry DATE
);

CREATE TABLE sectors (
  id SERIAL PRIMARY KEY,
  name VARCHAR(50),
  size bigint,
  dateOfEntry DATE
);
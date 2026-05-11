-- 1. Patients
CREATE OR REPLACE EXTERNAL TABLE `project-8bf7907a-2104-49b0-99f.bronze_dataset.hospital-a_patients`
OPTIONS (
  format = 'NEWLINE_DELIMITED_JSON',
  uris = ['gs://healthcare-tungpd20/landing/hospital-a/patients/*.json']
);

CREATE OR REPLACE EXTERNAL TABLE `project-8bf7907a-2104-49b0-99f.bronze_dataset.hospital-b_patients`
OPTIONS (
  format = 'NEWLINE_DELIMITED_JSON',
  uris = ['gs://healthcare-tungpd20/landing/hospital-b/patients/*.json']
);

-- 2. Encounters
CREATE OR REPLACE EXTERNAL TABLE `project-8bf7907a-2104-49b0-99f.bronze_dataset.hospital-a_encounters`
OPTIONS (
  format = 'NEWLINE_DELIMITED_JSON',
  uris = ['gs://healthcare-tungpd20/landing/hospital-a/encounters/*.json']
);

CREATE OR REPLACE EXTERNAL TABLE `project-8bf7907a-2104-49b0-99f.bronze_dataset.hospital-b_encounters`
OPTIONS (
  format = 'NEWLINE_DELIMITED_JSON',
  uris = ['gs://healthcare-tungpd20/landing/hospital-b/encounters/*.json']
);


-- 4. Providers
CREATE OR REPLACE EXTERNAL TABLE `project-8bf7907a-2104-49b0-99f.bronze_dataset.hospital-a_providers`
OPTIONS (
  format = 'NEWLINE_DELIMITED_JSON',
  uris = ['gs://healthcare-tungpd20/landing/hospital-a/providers/*.json']
);

CREATE OR REPLACE EXTERNAL TABLE `project-8bf7907a-2104-49b0-99f.bronze_dataset.hospital-b_providers`
OPTIONS (
  format = 'NEWLINE_DELIMITED_JSON',
  uris = ['gs://healthcare-tungpd20/landing/hospital-b/providers/*.json']
);

-- 5. Departments
CREATE OR REPLACE EXTERNAL TABLE `project-8bf7907a-2104-49b0-99f.bronze_dataset.hospital-a_departments`
OPTIONS (
  format = 'NEWLINE_DELIMITED_JSON',
  uris = ['gs://healthcare-tungpd20/landing/hospital-a/departments/*.json']
);

CREATE OR REPLACE EXTERNAL TABLE `project-8bf7907a-2104-49b0-99f.bronze_dataset.hospital-b_departments`
OPTIONS (
  format = 'NEWLINE_DELIMITED_JSON',
  uris = ['gs://healthcare-tungpd20/landing/hospital-b/departments/*.json']
);

-- 6. Transactions
CREATE OR REPLACE EXTERNAL TABLE `project-8bf7907a-2104-49b0-99f.bronze_dataset.hospital-a_transactions`
OPTIONS (
  format = 'NEWLINE_DELIMITED_JSON',
  uris = ['gs://healthcare-tungpd20/landing/hospital-a/transactions/*.json']
);

CREATE OR REPLACE EXTERNAL TABLE `project-8bf7907a-2104-49b0-99f.bronze_dataset.hospital-b_transactions`
OPTIONS (
  format = 'NEWLINE_DELIMITED_JSON',
  uris = ['gs://healthcare-tungpd20/landing/hospital-b/transactions/*.json']
);


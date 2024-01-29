variable "credentials" {
  description = "credentials"
  default     = "./keys/my-creds.json"
}

variable "project" {
  description = "Project ID"
  default     = "de-zoomcamp-411615"
}

variable "location" {
  description = "Deployment location"
  default     = "EU"
}
variable "bq_dataset_name" {
  description = "my bigquery dataset name"
  default     = "demo_dataset"
}

variable "gcs_bucket_name" {
  description = "Storage Bucket Name"
  default     = "terra-bucket-411615"
}

variable "gcs_storage_class" {
  description = "Bucket Storage Class"
  default     = "STANDARD"
}
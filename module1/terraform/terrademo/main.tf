terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "7.16.0"
    }
  }
}


provider "google" {
  # Configuration options
  credentials = "./keys/my-creds.json"
  project = "dtc-de-course-484903"
  region  = "us-central1" # ?? 
}

resource "google_storage_bucket" "demo-bucket" {
  name          = "dtc-de-course-484903-terra-bucket" # has to be globally unique 
  location      = "US"
  force_destroy = true

  lifecycle_rule {
    condition {
      age = 1 # days 
    }
    action {
      type = "Delete"
    }
  }

  lifecycle_rule {
    condition {
      age = 1
    }
    action {
      type = "AbortIncompleteMultipartUpload"
    }
  }
}
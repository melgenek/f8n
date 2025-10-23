terraform {
  required_providers {
    vultr = {
      source = "vultr/vultr"
      version = "2.27.1"
    }
  }
}

provider "vultr" {
  api_key = var.vultr_api_key
  # Defaults to US (NJ) if not specified
  # region = "ewr"
}

provider "azurerm" {
    features {}
}

data "azurerm_storage_account" "MyStorage" {
  name                = "datacohortworkspacelabs"
  resource_group_name = "rg-data-cohort-labs"
}

resource "azurerm_storage_container" "Landing" {
  name                  = "landing-phanai"
  storage_account_name  = "datacohortworkspacelabs"
  container_access_type = "private"
}

resource "azurerm_storage_container" "Bronze" {
  name                  = "bronze-phanai"
  storage_account_name  = "datacohortworkspacelabs"
  container_access_type = "private"
}

resource "azurerm_storage_container" "Silver" {
  name                  = "silver-phanai"
  storage_account_name  = "datacohortworkspacelabs"
  container_access_type = "private"
}

resource "azurerm_storage_container" "Gold" {
  name                  = "gold-phanai"
  storage_account_name  = "datacohortworkspacelabs"
  container_access_type = "private"
}
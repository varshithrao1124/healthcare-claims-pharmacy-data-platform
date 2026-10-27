using '../main.bicep'

param environment = 'prod'
param location = 'eastus'
param storageAccountName = 'sthcclaimsprod'
param keyVaultName = 'kv-hc-claims-prod'
param dataFactoryName = 'adf-hc-claims-prod'
param databricksName = 'dbw-hc-claims-prod'
param engineerPrincipalId = ''

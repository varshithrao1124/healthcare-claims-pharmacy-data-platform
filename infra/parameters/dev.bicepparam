using '../main.bicep'

param environment = 'dev'
param location = 'eastus'
param storageAccountName = 'sthcclaimsdev'
param keyVaultName = 'kv-hc-claims-dev'
param dataFactoryName = 'adf-hc-claims-dev'
param databricksName = 'dbw-hc-claims-dev'
param engineerPrincipalId = ''

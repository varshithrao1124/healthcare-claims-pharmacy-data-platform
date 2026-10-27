// Healthcare claims platform: ADLS Gen2, Key Vault, Data Factory, Databricks.
// Deploy-ready. This file has not been applied to a subscription in this repo.
//
//   az deployment group create \
//     --resource-group rg-hc-claims-dev \
//     --template-file infra/main.bicep \
//     --parameters infra/parameters/dev.bicepparam

targetScope = 'resourceGroup'

@description('Environment short name: dev, test, or prod')
param environment string

@description('Azure region')
param location string = resourceGroup().location

@description('Globally unique storage account name (lowercase, 3-24 chars)')
param storageAccountName string

@description('Key Vault name')
param keyVaultName string

@description('Data Factory name')
param dataFactoryName string

@description('Databricks workspace name')
param databricksName string

@description('Object id of the engineer group granted Key Vault secrets user')
param engineerPrincipalId string = ''

var tags = {
  application: 'healthcare-claims'
  environment: environment
  dataClassification: 'synthetic'
}

resource storage 'Microsoft.Storage/storageAccounts@2023-05-01' = {
  name: storageAccountName
  location: location
  tags: tags
  sku: {
    name: 'Standard_LRS'
  }
  kind: 'StorageV2'
  properties: {
    isHnsEnabled: true
    minimumTlsVersion: 'TLS1_2'
    allowBlobPublicAccess: false
    supportsHttpsTrafficOnly: true
  }
}

resource blobService 'Microsoft.Storage/storageAccounts/blobServices@2023-05-01' = {
  parent: storage
  name: 'default'
}

resource claimsContainer 'Microsoft.Storage/storageAccounts/blobServices/containers@2023-05-01' = {
  parent: blobService
  name: 'claims'
  properties: {
    publicAccess: 'None'
  }
}

resource sourceContainer 'Microsoft.Storage/storageAccounts/blobServices/containers@2023-05-01' = {
  parent: blobService
  name: 'source-landing'
  properties: {
    publicAccess: 'None'
  }
}

resource keyVault 'Microsoft.KeyVault/vaults@2023-07-01' = {
  name: keyVaultName
  location: location
  tags: tags
  properties: {
    tenantId: subscription().tenantId
    sku: {
      family: 'A'
      name: 'standard'
    }
    enableRbacAuthorization: true
    enableSoftDelete: true
    softDeleteRetentionInDays: 7
    publicNetworkAccess: 'Enabled'
  }
}

resource factory 'Microsoft.DataFactory/factories@2018-06-01' = {
  name: dataFactoryName
  location: location
  tags: tags
  identity: {
    type: 'SystemAssigned'
  }
  properties: {}
}

resource storageRole 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid(storage.id, factory.id, 'ba92f5b4-2d11-453d-a403-e96b0029c9fe')
  scope: storage
  properties: {
    roleDefinitionId: subscriptionResourceId('Microsoft.Authorization/roleDefinitions', 'ba92f5b4-2d11-453d-a403-e96b0029c9fe')
    principalId: factory.identity.principalId
    principalType: 'ServicePrincipal'
  }
}

resource accessConnector 'Microsoft.Databricks/accessConnectors@2023-05-01' = {
  name: 'databricks-access-connector'
  location: location
  tags: tags
  identity: {
    type: 'SystemAssigned'
  }
}

resource connectorStorageRole 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid(storage.id, accessConnector.id, 'ba92f5b4-2d11-453d-a403-e96b0029c9fe')
  scope: storage
  properties: {
    roleDefinitionId: subscriptionResourceId('Microsoft.Authorization/roleDefinitions', 'ba92f5b4-2d11-453d-a403-e96b0029c9fe')
    principalId: accessConnector.identity.principalId
    principalType: 'ServicePrincipal'
  }
}

resource databricks 'Microsoft.Databricks/workspaces@2024-05-01' = {
  name: databricksName
  location: location
  tags: tags
  sku: {
    name: 'premium'
  }
}

output storageAccountId string = storage.id
output storageBlobEndpoint string = storage.properties.primaryEndpoints.dfs
output keyVaultName string = keyVault.name
output dataFactoryPrincipalId string = factory.identity.principalId
output accessConnectorPrincipalId string = accessConnector.identity.principalId
output databricksWorkspaceId string = databricks.id
output claimsContainerName string = claimsContainer.name
output sourceContainerName string = sourceContainer.name
output engineerPrincipalConfigured bool = engineerPrincipalId != ''

Param(
    [Parameter(Mandatory=$true)] [string]   $SubscriptionId,
    [Parameter(Mandatory=$true)] [string]   $ResourceGroup,
    [Parameter(Mandatory=$true)] [string]   $DataFactory,
    [Parameter(Mandatory=$true)] [DateTime] $Since
  )
   
  # Login to Azure
  try {
      Get-AzureRmContext;
  } catch {
      Login-AzureRmAccount;
  }
   
  # Change to the proper subscription
  Select-AzureRmSubscription -SubscriptionId $SubscriptionId;
   
  # Get a reference to the ADF
  $df = Get-AzureRmDataFactory -ResourceGroupName $ResourceGroup -Name $DataFactory
   
  # Get all files
  $files = (Get-ChildItem -Path . -Filter *.json | ? { $_.LastWriteTime -gt $Since });
   
  # Upload all linked services
  ForEach ($file in $files) {
    $type = $file.Name.Split("-")[3];
    if ($type -eq "ls") {
      New-AzureRmDataFactoryLinkedService -DataFactory $df -File $file.FullName -Force
    }
  }
   
  # Upload all datasets
  ForEach ($file in $files) {
    $type = $file.Name.Split("-")[3];
    if ($type -eq "ds") {
      New-AzureRmDataFactoryDataset -DataFactory $df -File $file.FullName -Force
    }
  }
   
  # Upload all pipelines
  ForEach ($file in $files) {
    $type = $file.Name.Split("-")[3];
    if ($type -eq "pl") {
      New-AzureRmDataFactoryPipeline -DataFactory $df -File $file.FullName -Force
    }
  }
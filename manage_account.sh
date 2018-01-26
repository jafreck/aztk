#!/bin/sh
# Parse arguments

programname=$0
print_usage() {
    echo "-----------------"
    echo "$programname"
    echo "-----------------"
    echo ""
    echo "Management tool for the resources required to use aztk"
    echo ""
    echo "Commands:"
    echo ""
    echo "  create:     creates a new set of resources required for aztk"
    echo "  get-keys:   get the keys required to run aztk in json format"
    echo ""
    echo "Usage:"
    echo "  create <region> [resource_group] [batch_account] [storage_account]"
    echo "      username:           <required>"
    echo "      password:           <required>"
    echo "      region:             <required>"
    echo "      resource_group:     [optional: default = 'aztk']"
    echo "      batch_account:      [optional: default = 'aztk_batch']"
    echo "      storage_account:    [optional: default = 'aztk_storage']"
    echo "      service_principal:  [optional: default = 'aztk_service_principal']"

    echo ""
    echo "  get-keys [resource_group] [batch_account] [storage_account]"
    echo "      username:           <required>"
    echo "      password:           <required>"
    echo "      resource_group:   [optional: default = 'aztk']"
    echo "      batch_account:    [optional: default = 'aztk_batch']"
    echo "      storage_account:  [optional: default = 'aztk_storage']"
    echo ""
    echo ""
    echo "Examples"
    echo "   $programname create username password westus"
    echo "   $programname create username password westus resource_group_name batch_account_name storage_account_name"
    echo "   $programname get-keys"
    echo "   $programname get-keys username password resource_group_name batch_account_name storage_account_name"
}

login() {
    username=$1
    password=$2

    # Login using CLI
    echo "Logging in as $username."
    az login
    # az login --username $username --password $password
}

# Parameters
# $1 service principal
create_aad_app() {
    aad_app_name=$1

    # Create AAD App
    echo "Creating Azure Active Directory App with Name $aad_app_name"
    az ad app create --display-name $aad_app_name --homepage "https://aztk.local" --identifier-uri "https://test-aztk-uri" -o table
}

# Parameters
# $1 password
# $2 aad app name
# $3 region
# $4 resource group
# $5 batch account
# $6 storage account
create_accounts() {
    password=$1
    aad_app_name=$2
    location=$3
    resource_group=$4
    batch_account=$5
    storage_account=$6

    # Create resource group
    echo "Creating resource group."
    az group create -n $resource_group -l $location -o table

    # Create storage account
    echo "\nCreating storage account."
    az storage account create --name $storage_account --sku Standard_LRS --location $location --resource-group $resource_group -o table

    # Create batch account
    echo "\nCreating batch account."
    az batch account create --name $batch_account --location $location --resource-group $resource_group --storage-account $storage_account -o table

    # Create service principal with batch and storage account permsissions
    echo "\nCreating service princpal"
    batch_account_resource_id = "$(az batch account show --name $batch_account --resource-group $resource_group | jq '{key: .id}')"
    storage_account_resource_id = "$(az storage account show --name $storage_account --resource-group $resource_group | jq '{key: .id}')"
    az ad sp create-for-rbac --name $aad_app_name --password $password --scopes $batch_account_resource_id $storage_account_resource_id

   echo "\nDone creating accounts. Run '$0 get-keys' to view your account credentials."
}

# Parameters
# $1 resource group
# $2 batch account
# $3 storage account
get_credentials() {
    # Get keys and urls
    resource_group=$4
    batch_account=$5
    storage_account=$6

    batch_account_key="$(az batch account keys list \
            --name $batch_account_name \
            --resource-group $resource_group \
            | jq '{key: .primary}' | jq .[])"
    batch_account_url="$(az batch account list \
            --resource-group $resource_group \
            | jq .[0].accountEndpoint)"
    storage_account_key="$(az storage account keys list \
            --account-name $storage_account_name \
            --resource-group $resource_group \
            | jq '.[0].value')"
    storage_account_url="$(az storage account show \
        --resource-group $resource_group \
        --name $storage_account_name \
        | jq .primaryEndpoints.blob)"

    export JSON='{\n
        "batchAccount": { \n
            \t"name": "'"$batch_account_name"'", \n
            \t"key": '$batch_account_key', \n
            \t"url": '$batch_account_url' \n
        }, \n
        "storageAccount": { \n
            \t"name": "'"$storage_account_name"'", \n
            \t"key": '$storage_account_key', \n
            \t"url": '$storage_account_url' \n
        }\n}'
    echo $JSON
}


# Main program
if [ "$#" -eq 0 ]; then
    # No parameters
    print_usage
    exit 1
fi

COMMAND=$1
username=$2
password=$3
location=$4
resource_group=$5
batch_account_name=$6
storage_account_name=$7
service_principal_name=$8

login $username $password

if [ "$COMMAND" = "create" ]; then
    # Handle 'create' command
    location=$2

    if [ "$location" = "" ]; then
        echo "missing required input 'location'"
        print_usage
        exit 1
    fi

    # Set defaults
    if [ "$resource_group" = "" ]; then
        resource_group="aztk_resource_group"
        echo "using default resource group: $resource_group"
    fi

    if [ "$batch_account_name" = "" ]; then
        batch_account_name="aztk_batch"
        echo "using default batch_account_name: $batch_account_name"
    fi

    if [ "$storage_account_name" = "" ]; then
        storage_account_name="aztk_storage"
        echo "using default storage_account_name: $storage_account_name"
    fi

    if [ "$service_principal_name" = "" ]; then
        service_principal_name="aztk_service_principal"
        echo "using default service_principal_name: $service_principal_name"
    fi
    
    create_aad_app $service_principal_name
    create_accounts $password $service_principal_name $location $resource_group $batch_account_name $storage_account_name 
    
    exit 0
fi

if [ "$COMMAND" = "get-keys"  ]; then
    # Handle 'get-keys' command

    # Set defaults
    if [ "$resource_group" = "" ]; then
        resource_group="aztk"
    fi

    if [ "$batch_account_name" = "" ]; then
        batch_account_name="aztk_batch"
    fi

    if [ "$storage_account_name" = "" ]; then
        storage_account_name="aztk_storage"
    fi
    if [ "$service_principal_name" = "" ]; then
        service_principal_name="aztk_service_principal"
    fi

    get_credentials $resource_group $batch_account_name $storage_account_name
    exit 0
fi

if [ "$COMMAND" = "-h" ] || [ "$COMMAND" = "--help" ] || [ "$COMMAND" = "h" ] |
    [ "$COMMAND" = "help" ] || [ "$COMMAND" = "?" ]; then
    # Handle 'help' command
    print_usage
    exit 0
fi

if [ "$COMMAND" != "" ]; then
    # Handle unknown commands
    echo "Unknown command '$COMMAND'"
    print_usage
    exit 1
fi

exit 0
package main

import (
	"encoding/base64"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"runtime"

	"github.com/Azure/azure-sdk-for-go/arm/resources/resources"
	"github.com/Azure/azure-sdk-for-go/arm/storage"
	simpleStorage "github.com/Azure/azure-sdk-for-go/storage"
	"github.com/Azure/go-autorest/autorest"
	"github.com/Azure/go-autorest/autorest/adal"
	"github.com/Azure/go-autorest/autorest/azure"
	"github.com/Microsoft/azure-vhd-utils/upload"
	uploadMetaData "github.com/Microsoft/azure-vhd-utils/upload/metadata"
	"github.com/Microsoft/azure-vhd-utils/vhdcore/common"
	"github.com/Microsoft/azure-vhd-utils/vhdcore/diskstream"
	"github.com/Microsoft/azure-vhd-utils/vhdcore/validator"
)

const (
	defaultLocation             = "westeurope"
	defaultAccountName          = "linuxkit"
	defaultStorageContainerName = "linuxkitcontainer"
	defaultStorageBlobName      = "linuxkitimage.vhd"
)

var (
	groupsClient        resources.GroupsClient
	simpleStorageClient simpleStorage.Client
	accountsClient      storage.AccountsClient

	defaultActiveDirectoryEndpoint = azure.PublicCloud.ActiveDirectoryEndpoint
	defaultResourceManagerEndpoint = azure.PublicCloud.ResourceManagerEndpoint
)

func push(args []string) {
	flags := flag.NewFlagSet("push-azure", flag.ExitOnError)
	invoked := filepath.Base(os.Args[0])

	flags.Usage = func() {
		fmt.Printf("USAGE: %s push [options] imagePath\n\n", invoked)
		fmt.Printf("'imagePath' specified the path (absolute or relative) of a\n")
		fmt.Printf("VHD image to be pushed in an account storage\n\n")
		flags.PrintDefaults()
	}

	resourceGroupName := flags.String("resourceGroupName", "", "Name of the resource group where to upload the image")
	location := flags.String("location", defaultLocation, "Location of the storage account to upload the image")
	accountName := flags.String("accountName", defaultAccountName, "Name of the storage account")

	if err := flags.Parse(args); err != nil {
		log.Fatalf("Unable to parse args: %v", err)
	}

	fmt.Printf("Invoked with args: %s, %s, %s", *resourceGroupName, *location, *accountName)
}

func initializeAzureClients(subscriptionID, tenantID, clientID, clientSecret string) {
	oAuthConfig, err := adal.NewOAuthConfig(defaultActiveDirectoryEndpoint, tenantID)
	if err != nil {
		log.Fatalf("Cannot get oAuth configuration: %v", err)
	}

	token, err := adal.NewServicePrincipalToken(*oAuthConfig, clientID, clientSecret, defaultResourceManagerEndpoint)
	if err != nil {
		log.Fatalf("Cannot get service principal token: %v", err)
	}

	groupsClient = resources.NewGroupsClient(subscriptionID)
	groupsClient.Authorizer = autorest.NewBearerAuthorizer(token)

	accountsClient = storage.NewAccountsClient(subscriptionID)
	accountsClient.Authorizer = autorest.NewBearerAuthorizer(token)

}

func uploadVMImage(resourceGroupName string, accountName string, imagePath string) {

	const PageBlobPageSize int64 = 2 * 1024 * 1024
	parallelism := 8 * runtime.NumCPU()

	accountKeys, err := accountsClient.ListKeys(resourceGroupName, accountName)
	if err != nil {
		log.Fatalf("Unable to retrieve storage account key: %v", err)
	}

	keys := *(accountKeys.Keys)

	absolutePath, err := filepath.Abs(imagePath)
	if err != nil {
		log.Fatalf("Unable to get absolute path: %v", err)
	}

	ensureVHDSanity(absolutePath)

	diskStream, err := diskstream.CreateNewDiskStream(absolutePath)
	if err != nil {
		log.Fatalf("Unable to create disk stream for VHD: %v", err)
	}
	defer diskStream.Close()

	simpleStorageClient, err = simpleStorage.NewBasicClient(accountName, *keys[0].Value)
	if err != nil {
		log.Fatalf("Unable to create simple storage client: %v", err)
	}

	blobServiceClient := simpleStorageClient.GetBlobService()
	_, err = blobServiceClient.CreateContainerIfNotExists(defaultStorageContainerName, simpleStorage.ContainerAccessTypePrivate)
	if err != nil {
		log.Fatalf("Unable to create or retrieve container: %v", err)
	}

	localMetaData := getLocalVHDMetaData(absolutePath)

	err = blobServiceClient.PutPageBlob(defaultStorageContainerName, defaultStorageBlobName, diskStream.GetSize(), nil)
	if err != nil {
		log.Fatalf("Unable to create VHD blob: %v", err)
	}

	m, _ := localMetaData.ToMap()
	err = blobServiceClient.SetBlobMetadata(defaultStorageContainerName, defaultStorageBlobName, m, make(map[string]string))
	if err != nil {
		log.Fatalf("Unable to set blob metatada: %v", err)
	}

	var rangesToSkip []*common.IndexRange
	uploadableRanges, err := upload.LocateUploadableRanges(diskStream, rangesToSkip, PageBlobPageSize)
	if err != nil {
		log.Fatalf("Unable to locate uploadable ranges: %v", err)
	}

	uploadableRanges, err = upload.DetectEmptyRanges(diskStream, uploadableRanges)
	if err != nil {
		log.Fatalf("Unable to detect empty blob ranges: %v", err)
	}

	cxt := &upload.DiskUploadContext{
		VhdStream:             diskStream,
		UploadableRanges:      uploadableRanges,
		AlreadyProcessedBytes: common.TotalRangeLength(rangesToSkip),
		BlobServiceClient:     blobServiceClient,
		ContainerName:         defaultStorageContainerName,
		BlobName:              defaultStorageBlobName,
		Parallelism:           parallelism,
		Resume:                false,
		MD5Hash:               localMetaData.FileMetaData.MD5Hash,
	}

	err = upload.Upload(cxt)
	if err != nil {
		log.Fatalf("Unable to upload VHD: %v", err)
	}

	setBlobMD5Hash(blobServiceClient, defaultStorageContainerName, defaultStorageBlobName, localMetaData)

}

func getEnvVarOrExit(varName string) string {
	value := os.Getenv(varName)
	if value == "" {
		log.Fatalf("Missing environment variable %s\n", varName)
	}

	return value
}

func ensureVHDSanity(localVHDPath string) {
	if err := validator.ValidateVhd(localVHDPath); err != nil {
		log.Fatalf("Unable to validate VHD: %v", err)
	}

	if err := validator.ValidateVhdSize(localVHDPath); err != nil {
		log.Fatalf("Unable to validate VHD size: %v", err)
	}
}

func getLocalVHDMetaData(localVHDPath string) *uploadMetaData.MetaData {
	localMetaData, err := uploadMetaData.NewMetaDataFromLocalVHD(localVHDPath)
	if err != nil {
		log.Fatalf("Unable to get VHD metadata: %v", err)
	}
	return localMetaData
}

func setBlobMD5Hash(client simpleStorage.BlobStorageClient, containerName, blobName string, vhdMetaData *uploadMetaData.MetaData) {
	if vhdMetaData.FileMetaData.MD5Hash != nil {
		blobHeaders := simpleStorage.BlobHeaders{
			ContentMD5: base64.StdEncoding.EncodeToString(vhdMetaData.FileMetaData.MD5Hash),
		}
		if err := client.SetBlobProperties(containerName, blobName, blobHeaders); err != nil {
			log.Fatalf("Unable to set blob properties: %v", err)
		}
	}
}

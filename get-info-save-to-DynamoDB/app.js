const { S3Client, ListObjectsV2Command, GetObjectCommand, DeleteObjectCommand, CopyObjectCommand } = require('@aws-sdk/client-s3');
const { DynamoDBClient, PutItemCommand } = require("@aws-sdk/client-dynamodb");


    // Initialize the S3 client
    const s3Client = new S3Client({ region: 'us-east-1' }); // Replace 'us-east-1' with your desired region
    // Initialize the DynamoDB client
    const client = new DynamoDBClient({ region: "us-east-1" }); // Replace "your-region" with your AWS region
    // List all objects in the bucket
    const bucketName = process.env.BUCKET_NAME
exports.handler = async (event) => {
    console.log(JSON.stringify(event))

    try {

        const Files = await getFilesFromBucket();

        for (const file of Files) {
            console.log('Getting file information...')
            const fileContentString = await readFilesInfo(file);
            console.log(`File content String: ${fileContentString} ` );
            const fileData = await parseDataToJson(fileContentString)
            console.log(`File Parse JSON: ${fileData.location.name}`);
            const dataInserted = await storeDataInDynamoDB(file, fileData);
            console.log("Item inserted successfully:", dataInserted);
            const copyData = await copyFile(file)
            console.log("Object copied successfully:", copyData);
            const deleteData = await deleteFile(file)
            console.log("Original object deleted successfully:", deleteData);

        }

    return {
        statusCode: 200,
        body: JSON.stringify({ message: 'Files processed successfully!' }),
        };

    } catch (error) {
        console.error('Error:', error);
        return { statusCode: 500, body: JSON.stringify({ message: 'An error occurred while processing the files', error: error }) };
    }
};

getFilesFromBucket = async() => {

    const listObjectsParams = { Bucket: bucketName };
    const listObjectsCommand = new ListObjectsV2Command(listObjectsParams);
    const { Contents } = await s3Client.send(listObjectsCommand);
    //console.log(Contents);
    return Contents;

}

readFilesInfo = async (file) => {

    // Retrieve the object from S3
    const getObjectParams = { Bucket: bucketName, Key: file.Key };
    const getObjectCommand = new GetObjectCommand(getObjectParams);
    const { Body } = await s3Client.send(getObjectCommand);

    // Convert the response body from a stream to a string
    return await streamToString(Body);

}

storeDataInDynamoDB = async (file, fileContent) => {

    const { city, isoDate } = splitAndConvertToISO8601(file.Key);
    console.log(`city value: ${city}`)
    // Example JSON data
    const jsonData = {
        City: { S: `${city}` }, // Partition key
        Date: { S: `${isoDate}` }, // Sort key\
        temp_f: { S: `${fileContent.current.temp_f}` },
        is_day: { S: `${fileContent.current.is_day}` },
        condition: { S: `${fileContent.current.condition.text}` },
        wind_kph: { S: `${fileContent.current.wind_kph}` },
        wind_dir: { S: `${fileContent.current.wind_dir}` },
        pressure_mb: { S: `${fileContent.current.pressure_mb}` },
        precip_mm: { S: `${fileContent.current.precip_mm}` },
        humidity: { S: `${fileContent.current.humidity}` },
        feelslike_f: { S: `${fileContent.current.feelslike_f}` }
    };

    // Prepare the PutItemCommand
    const params = {
        TableName: "test-weather-app", // Replace "YourTableName" with your actual table name
        Item: jsonData
    };
    const putCommand = new PutItemCommand(params);
    return await client.send(putCommand);
}


copyFile = async (sourceKey) => {

    const copyParams = {
        Bucket: bucketName,
        CopySource: `/${bucketName}/${sourceKey}`,
        Key: `/${bucketName}/${sourceKey}`
    };

    // Copy the object to the new folder
    const copyCommand = new CopyObjectCommand(copyParams);

    return await client.send(copyCommand);

}

deleteFile = async (sourceKey) => {
    
    const deleteParams = {
        Bucket: bucketName,
        Key: `/${bucketName}/${sourceKey}`
    };

    const deleteCommand = new DeleteObjectCommand(deleteParams);

    return await client.send(deleteCommand);

}


// Function to read the content from a readable stream and return it as a string
async function streamToString(stream) {
    const chunks = [];
    for await (const chunk of stream) {
        chunks.push(chunk);
    }
    return Buffer.concat(chunks).toString('utf-8');
}

// Function to parse fileString information to JSON
parseDataToJson = async (fileContent) => {
    return await JSON.parse(fileContent);
}

// Function to read the file split it and convert the date to ISO 8601

const splitAndConvertToISO8601 = (key) => {
    // Split the key into two parts: city and numeric part
    const [city, numericPart] = key.split('-');

    // Convert the numeric part to milliseconds
    const milliseconds = parseInt(numericPart) * 1000;

    // Create a new Date object using the milliseconds
    const date = new Date(milliseconds);

    // Convert the date to ISO 8601 format
    const isoDate = date.toISOString();

    return { city, isoDate };
};
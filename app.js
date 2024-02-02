const axios = require('axios');
const { S3Client, PutObjectCommand } = require('@aws-sdk/client-s3');

const s3 = new S3Client();
const bucketName = 'weather-report-florida-app';
let cities = ['Miami', 'Aventura', 'Doral' , 'Fort Lauderdale']

exports.handler = async (event) => {
    console.log('TEST');
      for (const iterator of cities) {
         let dataResponse = await readWeatherInformation(iterator);
         await writeInfoToFile(bucketName , JSON.stringify(dataResponse.location.name) +'-'+ JSON.stringify(dataResponse.location.localtime_epoch , JSON.stringify(dataResponse)))
      }  
}

readWeatherInformation = async (city) => {

    let config = {
        method: 'get',
        maxBodyLength: Infinity,
        url: 'http://api.weatherapi.com/v1/current.json?key=5853f867157c4d1cbfa204841243101&q='+city+'&aqi=no',
        headers: { }
      };
      return await axios.request(config).then((response) => {
        let responseData = response.data;
        
        return responseData;

      }).catch((error) => {

        console.log(error);
      });


}

writeInfoToFile = (bucketName, fileName, dataToWrite) => {

    const params = {
        Bucket: bucketName,
        Key: fileName,
        Body: dataToWrite,
      };

      try {
        s3.send(new PutObjectCommand(params));
        console.log(`File "${fileName}" has been created and data has been written successfully to S3 bucket "${bucketName}"`);
    
        return {
          statusCode: 200,
          body: JSON.stringify({ message: 'File created successfully!' }),
        };
      } catch (error) {
        console.error('Error writing to S3:', error);
        
        return {
          statusCode: 500,
          body: JSON.stringify({ message: 'Internal Server Error' }),
        };
      }

}
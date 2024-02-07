const axios = require('axios');
const { S3Client, PutObjectCommand } = require('@aws-sdk/client-s3');

const s3 = new S3Client();
const bucketName = process.env.BUCKET_NAME
let cities = ['Miami', 'Aventura', 'Doral' , 'Fort Lauderdale']

exports.handler = async (event) => {
  console.log()  
  let fileName;
  try {
    for (const city of cities) {
      console.log('Getting city information...');
      let dataResponse = await readWeatherInformation(city);
      console.log(`City information: ${JSON.stringify(dataResponse)}`);
      fileName = `${dataResponse.location.name}-${dataResponse.location.localtime_epoch}`;
      console.log('Writing file...');
      let writtenFile = await writeInfoToFile(bucketName , fileName , dataResponse)
      console.log(`File inforamtion: ${JSON.stringify(writtenFile)}`);
    } 

    return {
      statusCode: 200,
      body: JSON.stringify({ message: 'Files created successfully!' }),
    };


  } catch (error) {
    console.error('Error: ', error);
        

    
  }
 
}

readWeatherInformation = async (city) => {

    let config = {
        method: 'get',
        maxBodyLength: Infinity,
        url: `http://api.weatherapi.com/v1/current.json?key=5853f867157c4d1cbfa204841243101&q=${city}&aqi=no`,
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

      return s3.send(new PutObjectCommand(params));

}
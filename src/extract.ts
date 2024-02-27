import https from 'node:https';
import { JSDOM } from 'jsdom';

/**
 * Define the data source
 */
const requestOptions: https.RequestOptions = {
    host: 'www.sir.toscana.it',
    port: 443,
    path: '/monitoraggio/stazioni.php?type=idro'
};

/**
 * Parses hydrometric request data to produce an array
 * of measurements
 * 
 * @param inputMeasurements 
 */
function parseHydrometricRequestData(inputMeasurements: RawSIRMeasurements) {
    let parsedMeasurements: Array<Measurement> = [];
    inputMeasurements.forEach((inputMeasurement) => {
        console.log({
            timestamp: inputMeasurement[13],
            value: inputMeasurement[6],
            unit: 'm',
            type: 'hydrometric'
        });
        const newMeasurement : Measurement = {
            timestamp: 0,
            /** As a float */
            value: parseFloat(inputMeasurement[6]),
            unit: 'm',
            type: 'hydrometric'
        };
        parsedMeasurements.push(newMeasurement);
    });
    //console.log(inputMeasurements);
    console.log(parsedMeasurements);
    //TODO: Push each measurement into the daily files for
    // a given sensor, river and province
}

/**
 * Fetch the page content to get the values array defining
 * function. While using a headless browser manager such as
 * puppeteer would have been easier, this would have inflated
 * the system requirements (and cost) immensely.
 */
https.get(requestOptions, (res) => {
    let pageContent = '';
    res.on('data', (d) => {
        pageContent += d.toString();
    });
    res.on('end', () => {
        try {
            const domObject = new JSDOM(pageContent, { runScripts: "dangerously" });
            const hydrometricRequests = parseHydrometricRequestData(domObject.window.VALUES);
        } catch (error) {
            //console.err(error.msg);
        }
        console.log("Data parsed successfully!");
    })
}).on('error', function(e) {
    console.log("Got request error: " + e.message);
});

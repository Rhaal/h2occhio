import https from 'node:https';
import fs from 'node:fs';
import { JSDOM } from 'jsdom';
import dayjs from 'dayjs';
import customParseFormat from 'dayjs/plugin/customParseFormat';
import utc from 'dayjs/plugin/utc';
import timezone from 'dayjs/plugin/timezone';
import jsonfile from 'jsonfile';

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
    // We'll need to extend dayjs to support arbitrary date formats in a
    // non-UTC timezone
    dayjs.extend(customParseFormat);
    dayjs.extend(utc);
    dayjs.extend(timezone);
    const currentYear = dayjs().year();
    const baseMeasurementStoragePath = 'docs/data/';
    if (!fs.existsSync(baseMeasurementStoragePath)) {
        fs.mkdirSync(baseMeasurementStoragePath, { recursive: true });
    }
    const indexingFolders = [
        'sensors', 'rivers', 'provinces'
    ];
    indexingFolders.forEach((indexingFolder) => {
        const indexingFolderPath = `${baseMeasurementStoragePath}/${indexingFolder}`;
        if (!fs.existsSync(indexingFolderPath)) {
            fs.mkdirSync(indexingFolderPath, { recursive: true });
        }
    });
    inputMeasurements.forEach((inputMeasurement) => {
        const measurementDate = dayjs.tz(
            `${currentYear}/${inputMeasurement[13]}`,
            'YYYY/DD/MM HH.mm',
            'Europe/Rome'
        );
        const newMeasurement : Measurement = {
            sensor_id: inputMeasurement[0],
            timestamp: measurementDate.unix(),
            /** As a float */
            value: inputMeasurement[6].length > 0 ?
                parseFloat(inputMeasurement[6]) :
                0,
            unit: 'm',
            type: 'hydrometric'
        };
        parsedMeasurements.push(newMeasurement);
        const indexingFolders = [
            'sensors', 'rivers', 'provinces'
        ];
        // Push each measurement into the daily files for
        // a given sensor, river and province
        indexingFolders.forEach((indexingFolder) => {
            let indexingKey = null;
            switch (indexingFolder) {
                case 'rivers':
                    indexingKey = inputMeasurement[1];
                    break;
                case 'provinces':
                    indexingKey = inputMeasurement[3];
                    break;
                case 'sensors':
                default:
                    indexingKey = inputMeasurement[0];
                    break;
            }
            const indexingFolderPath = `${baseMeasurementStoragePath}/${indexingFolder}/` +
                `${indexingKey}/${currentYear}/${measurementDate.month() + 1}`;
            if (!fs.existsSync(indexingFolderPath)) {
                fs.mkdirSync(indexingFolderPath, { recursive: true });
            }
            const filePath = `${indexingFolderPath}/${measurementDate.date()}.json`;
            jsonfile.writeFileSync(filePath, newMeasurement, { flag: 'a' });
        })
    });
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
